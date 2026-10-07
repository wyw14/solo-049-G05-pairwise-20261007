package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"orbitlink/internal/audit"
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
	for _, target := range []string{"/passes", "/antennas", "/rf-chains", "/recordings", "/healthz", "/api/passes", "/api/antennas", "/api/rf-chains", "/api/recordings", "/api/clock/corrections", "/api/clock/quality", "/api/clock/reviews"} {
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
	if _, err := runtime.Clock.Apply(stationclock.Sample{ObservedAt: time.Now().UTC(), Offset: 2 * time.Second, Source: "gps", Quality: "locked"}); err != nil {
		t.Fatal(err)
	}
	if len(runtime.Clock.Corrections()) != 1 {
		t.Fatal("clock correction was not persisted")
	}
	if err := runtime.RF.EnableTransmit("RF-01"); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.Weather.Handle(context.Background(), weather.WindSample{StationID: "station", GustMetersPerSecond: 25, ObservedAt: time.Now().UTC()}, value.ID, value.AntennaID, "RF-01")
	if err != nil || !result.InhibitConfirmed || !result.Stowed {
		t.Fatalf("wind safety did not converge: %#v %v", result, err)
	}
}

func TestClockQualityReviewFlowAPI(t *testing.T) {
	runtime := newTestRuntime(t)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	client := server.Client()
	value := runtime.Passes.List()[0]
	deadlineBefore, found := runtime.Deadlines.Get(value.ID)
	if !found {
		t.Fatal("seeded pass has no deadline")
	}
	commandsBefore := map[string]time.Time{}
	for _, command := range runtime.Commands.List(value.ID) {
		commandsBefore[command.ID] = command.ExecuteAt
	}
	if len(commandsBefore) == 0 {
		t.Fatal("seeded pass has no pointing commands")
	}
	observed := time.Now().UTC().Format(time.RFC3339Nano)
	response := requestJSON(t, client, http.MethodPost, server.URL+"/api/clock/samples", `{"observed_at":"`+observed+`","offset":50000000,"source":"gps","quality":"locked"}`)
	var assessment stationclock.Assessment
	decodeBody(t, response, &assessment)
	if assessment.Decision != stationclock.DecisionAuto {
		t.Fatalf("small forward correction was not auto-applied: %#v", assessment)
	}
	response = requestJSON(t, client, http.MethodPost, server.URL+"/api/clock/samples", `{"observed_at":"`+observed+`","offset":5000000000,"source":"gps","quality":"locked"}`)
	decodeBody(t, response, &assessment)
	if assessment.Decision != stationclock.DecisionReview || assessment.ReviewID == "" {
		t.Fatalf("large correction was not routed to review: %#v", assessment)
	}
	if offset := runtime.Clock.Offset(); offset != 50*time.Millisecond {
		t.Fatalf("pending review changed the station offset to %v", offset)
	}
	deadline, _ := runtime.Deadlines.Get(value.ID)
	if !deadline.CivilLOS.Equal(deadlineBefore.CivilLOS.Add(50 * time.Millisecond)) {
		t.Fatalf("pending review moved the pass deadline to %v", deadline.CivilLOS)
	}
	for _, command := range runtime.Commands.List(value.ID) {
		if !command.ExecuteAt.Equal(commandsBefore[command.ID].Add(50 * time.Millisecond)) {
			t.Fatalf("pending review moved pointing command to %v", command.ExecuteAt)
		}
	}
	response = requestJSON(t, client, http.MethodGet, server.URL+"/api/clock/reviews?status=pending", "")
	var reviews struct {
		Items []stationclock.ReviewItem `json:"items"`
	}
	decodeBody(t, response, &reviews)
	if len(reviews.Items) != 1 || reviews.Items[0].ID != assessment.ReviewID {
		t.Fatalf("pending review not listed: %#v", reviews.Items)
	}
	response = requestJSON(t, client, http.MethodPost, server.URL+"/api/clock/reviews/"+assessment.ReviewID+"/decision", `{"approve":true,"decided_by":"test-operator"}`)
	var decided stationclock.ReviewItem
	decodeBody(t, response, &decided)
	if decided.Status != stationclock.ReviewApproved || decided.CorrectionID == "" {
		t.Fatalf("review was not approved: %#v", decided)
	}
	if offset := runtime.Clock.Offset(); offset != 5*time.Second {
		t.Fatalf("approved correction was not applied, offset %v", offset)
	}
	deadline, _ = runtime.Deadlines.Get(value.ID)
	if !deadline.CivilLOS.Equal(deadlineBefore.CivilLOS.Add(5 * time.Second)) {
		t.Fatalf("approved correction did not shift the planned deadline: %v", deadline.CivilLOS)
	}
	for _, command := range runtime.Commands.List(value.ID) {
		if !command.ExecuteAt.Equal(commandsBefore[command.ID].Add(5 * time.Second)) {
			t.Fatalf("approved correction did not shift pointing command: %v", command.ExecuteAt)
		}
	}
	actions := map[string]bool{}
	for _, event := range runtime.Audit.List(audit.Query{Limit: 500}) {
		actions[event.Component+"."+event.Action] = true
	}
	for _, expected := range []string{"clock.sample.evaluated", "clock.review.queued", "clock.review.approved", "scheduler.pointing.shifted"} {
		if !actions[expected] {
			t.Fatalf("audit trail is missing %s", expected)
		}
	}
}

func TestClockCorrectionSkipsActiveWindows(t *testing.T) {
	runtime := newTestRuntime(t)
	active := runtime.Passes.List()[0]
	if _, err := runtime.RF.Apply(rf.Plan{PassID: active.ID, ChainID: "RF-01", CenterHz: 2_245_000_000, DopplerHz: 12_000, Polarization: rf.RightCircular}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.RF.Apply(rf.Plan{PassID: active.ID, ChainID: "RF-02", CenterHz: 2_245_000_000, DopplerHz: 12_050, Polarization: rf.RightCircular}); err != nil {
		t.Fatal(err)
	}
	attempt, err := runtime.Attempts.Begin(active.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(active.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: active.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 3},
		{PassID: active.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 3},
	}
	if _, err := runtime.Passes.AcceptDiversity(active.ID, results); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Deadlines.Activate(active.ID, 8*time.Minute); err != nil {
		t.Fatal(err)
	}
	now := runtime.Clock.Now()
	planned, err := runtime.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-C07", AntennaID: "ANT-02", RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"}, AOS: now.Add(30 * time.Minute), LOS: now.Add(38 * time.Minute), Revision: 1, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.WindowRecompute.Apply(planned.ID, planned.Window); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Deadlines.Schedule(planned); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Replanner.QueueInitial(planned.ID); err != nil {
		t.Fatal(err)
	}
	activeDeadline, _ := runtime.Deadlines.Get(active.ID)
	plannedDeadline, _ := runtime.Deadlines.Get(planned.ID)
	activeCommands := map[string]time.Time{}
	for _, command := range runtime.Commands.List(active.ID) {
		activeCommands[command.ID] = command.ExecuteAt
	}
	plannedCommands := map[string]time.Time{}
	for _, command := range runtime.Commands.List(planned.ID) {
		plannedCommands[command.ID] = command.ExecuteAt
	}
	assessment, err := runtime.ClockMonitor.Submit(stationclock.Sample{ObservedAt: time.Now().UTC(), Offset: 100 * time.Millisecond, Source: "gps", Quality: "locked"})
	if err != nil {
		t.Fatal(err)
	}
	if assessment.Decision != stationclock.DecisionAuto {
		t.Fatalf("small forward correction was not auto-applied: %#v", assessment)
	}
	deadline, _ := runtime.Deadlines.Get(active.ID)
	if !deadline.CivilLOS.Equal(activeDeadline.CivilLOS) {
		t.Fatalf("correction moved the active pass deadline to %v", deadline.CivilLOS)
	}
	for _, command := range runtime.Commands.List(active.ID) {
		if !command.ExecuteAt.Equal(activeCommands[command.ID]) {
			t.Fatalf("correction moved an active pass pointing command to %v", command.ExecuteAt)
		}
	}
	deadline, _ = runtime.Deadlines.Get(planned.ID)
	if !deadline.CivilLOS.Equal(plannedDeadline.CivilLOS.Add(100 * time.Millisecond)) {
		t.Fatalf("correction did not shift the planned pass deadline: %v", deadline.CivilLOS)
	}
	for _, command := range runtime.Commands.List(planned.ID) {
		if !command.ExecuteAt.Equal(plannedCommands[command.ID].Add(100 * time.Millisecond)) {
			t.Fatalf("correction did not shift a planned pointing command: %v", command.ExecuteAt)
		}
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
