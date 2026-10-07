package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	stationclock "orbitlink/internal/clock"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/rf"
	"orbitlink/internal/satellite"
	"orbitlink/internal/weather"
)

func TestOperationsPagesAndAPIsUseRuntimeState(t *testing.T) {
	runtime := newTestRuntime(t)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	for _, target := range []string{"/passes", "/antennas", "/rf-chains", "/recordings", "/healthz", "/api/passes", "/api/antennas", "/api/rf-chains", "/api/recordings"} {
		response, err := server.Client().Get(server.URL + target)
		if err != nil {
			t.Fatalf("GET %s: %v", target, err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s returned %d", target, response.StatusCode)
		}
		_ = response.Body.Close()
	}
}

func TestPassRevisionAcquisitionAndRecordingFlow(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	if _, err := runtime.RF.Apply(rf.Plan{PassID: value.ID, ChainID: "RF-01", CenterHz: 2_245_000_000, DopplerHz: 12_000, Polarization: rf.RightCircular}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.RF.Apply(rf.Plan{PassID: value.ID, ChainID: "RF-02", CenterHz: 2_245_000_000, DopplerHz: 12_050, Polarization: rf.RightCircular}); err != nil {
		t.Fatal(err)
	}
	attempt, err := runtime.Attempts.Begin(value.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(value.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 7},
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 7},
	}
	decision, err := runtime.Passes.AcceptDiversity(value.ID, results)
	if err != nil || !decision.Ready {
		t.Fatalf("diversity did not converge: %#v %v", decision, err)
	}
	active, found := runtime.Recorder.ActiveForPass(value.ID)
	if !found {
		t.Fatal("tracking did not start a recorder")
	}
	session, err := runtime.NewDecoderSession(value.ID, active.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.OnLock(attempt.ID); err != nil {
		t.Fatal(err)
	}
	if written, err := session.Feed(attempt.ID, make([]byte, 128)); err != nil || written != 2 {
		t.Fatalf("decoder feed wrote %d frames: %v", written, err)
	}
	if _, err := runtime.Recorder.Drain(context.Background(), active.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Recorder.Seal(active.ID, "integration complete"); err != nil {
		t.Fatal(err)
	}
}

func TestEphemerisClockAndWindSafetyFlow(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	previous, _ := runtime.Ephemerides.Current(value.SatelliteID)
	if _, err := runtime.Ephemerides.Import(satellite.Ephemeris{SatelliteID: value.SatelliteID, Revision: previous.Revision + 1, Epoch: time.Now().UTC(), Line1: "revised orbital element one", Line2: "revised orbital element two"}); err != nil {
		t.Fatal(err)
	}
	commands := runtime.Commands.List(value.ID)
	if len(commands) == 0 || commands[len(commands)-1].Revision != previous.Revision+1 {
		t.Fatal("revised pointing commands were not queued")
	}
	// A small forward slew is applied automatically.
	if _, err := runtime.Clock.Apply(stationclock.Sample{ObservedAt: time.Now().UTC(), Offset: 50 * time.Millisecond, Source: "gps", Quality: "locked"}); err != nil {
		t.Fatal(err)
	}
	deadlineBefore, _ := runtime.Deadlines.Get(value.ID)
	pointingBefore := runtime.Commands.List(value.ID)
	// A large correction is held for manual review and leaves the offset untouched.
	held, err := runtime.ClockQuality.Ingest(stationclock.Sample{ObservedAt: time.Now().UTC(), Offset: 2 * time.Second, Source: "gps", Quality: "locked"})
	if err != nil {
		t.Fatal(err)
	}
	if held.Strategy != stationclock.StrategyReview || held.Review == nil {
		t.Fatalf("large correction was not held for review: %#v", held)
	}
	if runtime.Clock.Offset() != 50*time.Millisecond {
		t.Fatal("pending review must not change the live offset")
	}
	if deadlineMid, _ := runtime.Deadlines.Get(value.ID); !deadlineMid.CivilLOS.Equal(deadlineBefore.CivilLOS) {
		t.Fatal("pending review must not move pass deadlines")
	}
	impact, err := runtime.ClockQuality.Impact(held.Review.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(impact.Passes) == 0 {
		t.Fatal("review impact should list the seeded pass")
	}
	if _, _, err := runtime.ClockQuality.Approve(held.Review.ID, "operator", "verified against GNSS"); err != nil {
		t.Fatal(err)
	}
	if runtime.Clock.Offset() != 2*time.Second {
		t.Fatal("approved review did not apply the correction")
	}
	if len(runtime.Clock.Corrections()) != 2 {
		t.Fatal("clock corrections were not persisted")
	}
	// Approval propagates to the civil deadline and live pointing commands;
	// active windows remain monotonic (the seeded pass is still planned).
	delta := 2*time.Second - 50*time.Millisecond
	deadlineAfter, _ := runtime.Deadlines.Get(value.ID)
	if deadlineAfter.CivilLOS.Sub(deadlineBefore.CivilLOS) != delta {
		t.Fatalf("deadline moved by %s, want %s", deadlineAfter.CivilLOS.Sub(deadlineBefore.CivilLOS), delta)
	}
	for _, before := range pointingBefore {
		if before.Retired {
			continue
		}
		found := false
		for _, after := range runtime.Commands.List(value.ID) {
			if after.ID == before.ID {
				found = true
				if after.ExecuteAt.Sub(before.ExecuteAt) != delta {
					t.Fatalf("pointing %s moved by %s, want %s", after.ID, after.ExecuteAt.Sub(before.ExecuteAt), delta)
				}
			}
		}
		if !found {
			t.Fatalf("pointing command %s vanished after correction", before.ID)
		}
	}
	if err := runtime.RF.EnableTransmit("RF-01"); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.Weather.Handle(context.Background(), weather.WindSample{StationID: "station", GustMetersPerSecond: 25, ObservedAt: time.Now().UTC()}, value.ID, value.AntennaID, "RF-01")
	if err != nil || !result.InhibitConfirmed || !result.Stowed {
		t.Fatalf("wind safety did not converge: %#v %v", result, err)
	}
}

func TestRouterRejectsOverlappingReservation(t *testing.T) {
	runtime := newTestRuntime(t)
	first := runtime.Passes.List()[0]
	second, err := runtime.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-C07", AntennaID: first.AntennaID, RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"}, AOS: first.Window.AOS.Add(time.Minute), LOS: first.Window.LOS.Add(time.Minute), Revision: 1, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.WindowRecompute.Apply(second.ID, second.Window); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Reservations.ReservePass(second.ID, second.AntennaID); err == nil {
		t.Fatal("overlapping reservation unexpectedly succeeded")
	}
}

func TestClockReviewHTTPFlow(t *testing.T) {
	runtime := newTestRuntime(t)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()

	// Small forward slew applies straight away.
	response := requestJSON(t, server.Client(), http.MethodPost, server.URL+"/api/clock/samples",
		`{"source":"gnss","quality":"locked","offset":"20ms"}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("small slew returned %d", response.StatusCode)
	}
	response.Body.Close()

	// Large step is held: 202 with a review.
	response = requestJSON(t, server.Client(), http.MethodPost, server.URL+"/api/clock/samples",
		`{"source":"ntp","quality":"synced","offset_ns":2000000000}`)
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("large step returned %d, want 202", response.StatusCode)
	}
	var decision struct {
		Strategy string `json:"strategy"`
		Review   struct {
			ID string `json:"id"`
		} `json:"review"`
	}
	decodeBody(t, response, &decision)
	if decision.Strategy != "manual_review" || decision.Review.ID == "" {
		t.Fatalf("unexpected decision: %#v", decision)
	}

	impactResponse, err := server.Client().Get(server.URL + "/api/clock/reviews/" + decision.Review.ID + "/impact")
	if err != nil || impactResponse.StatusCode != http.StatusOK {
		t.Fatalf("impact fetch failed: %v %d", err, impactResponse.StatusCode)
	}
	impactResponse.Body.Close()

	approve := requestJSON(t, server.Client(), http.MethodPost,
		server.URL+"/api/clock/reviews/"+decision.Review.ID+"/approve",
		`{"reviewer":"ops-lead","note":"cross-checked"}`)
	if approve.StatusCode != http.StatusOK {
		t.Fatalf("approval returned %d", approve.StatusCode)
	}
	approve.Body.Close()

	correctionsResponse, err := server.Client().Get(server.URL + "/api/clock/corrections")
	if err != nil {
		t.Fatal(err)
	}
	var corrections struct {
		Items []struct {
			After int64 `json:"after"`
		} `json:"items"`
	}
	decodeBody(t, correctionsResponse, &corrections)
	last := corrections.Items[len(corrections.Items)-1]
	if last.After != int64(2*time.Second) {
		t.Fatalf("approved correction not applied: after=%d", last.After)
	}
}

func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func decodeBody(t *testing.T, response *http.Response, value any) {
	t.Helper()
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(value); err != nil {
		t.Fatal(err)
	}
}

func requestJSON(t *testing.T, client *http.Client, method, target, payload string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, target, strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
