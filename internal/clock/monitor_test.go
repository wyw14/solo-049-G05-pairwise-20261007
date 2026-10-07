package clock_test

import (
	"testing"
	"time"

	"orbitlink/internal/audit"
	stationclock "orbitlink/internal/clock"
	"orbitlink/internal/store"
)

func newMonitorFixture(t *testing.T) (*stationclock.Service, *stationclock.Monitor) {
	t.Helper()
	paths := store.NewPaths(t.TempDir())
	trail := audit.NewTrail(store.NewAppendLog(paths))
	service, err := stationclock.NewService(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	monitor, err := stationclock.NewMonitor(paths, service, trail, stationclock.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	return service, monitor
}

func sample(source string, offset time.Duration, at time.Time) stationclock.Sample {
	return stationclock.Sample{ObservedAt: at, Offset: offset, Source: source, Quality: "locked"}
}

func TestMonitorAutoAppliesSmallForwardCorrection(t *testing.T) {
	service, monitor := newMonitorFixture(t)
	var corrections []stationclock.Correction
	service.Subscribe(func(correction stationclock.Correction) {
		corrections = append(corrections, correction)
	})
	base := time.Now().UTC()
	for i := 0; i < 2; i++ {
		if _, err := monitor.Submit(sample("gps", 50*time.Millisecond, base.Add(time.Duration(i)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := monitor.Submit(sample("ntp", 54*time.Millisecond, base)); err != nil {
		t.Fatal(err)
	}
	assessment, err := monitor.Submit(sample("ntp", 54*time.Millisecond, base.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if assessment.Decision != stationclock.DecisionAuto {
		t.Fatalf("expected auto decision, got %s (%s)", assessment.Decision, assessment.Reason)
	}
	if service.Offset() != 52*time.Millisecond {
		t.Fatalf("expected ensemble offset 52ms, got %v", service.Offset())
	}
	if len(corrections) != 2 {
		t.Fatalf("expected two applied corrections, got %d", len(corrections))
	}
	report := monitor.Quality()
	if len(report.Sources) != 2 || report.PendingReviews != 0 {
		t.Fatalf("unexpected quality report: %#v", report)
	}
}

func TestMonitorQueuesLargeCorrectionForManualReview(t *testing.T) {
	service, monitor := newMonitorFixture(t)
	var corrections []stationclock.Correction
	service.Subscribe(func(correction stationclock.Correction) {
		corrections = append(corrections, correction)
	})
	base := time.Now().UTC()
	var reviewID string
	for i := 0; i < 2; i++ {
		assessment, err := monitor.Submit(sample("gps", 5*time.Second, base.Add(time.Duration(i)*time.Second)))
		if err != nil {
			t.Fatal(err)
		}
		reviewID = assessment.ReviewID
	}
	if reviewID == "" {
		t.Fatal("large correction was not routed to manual review")
	}
	if service.Offset() != 0 {
		t.Fatalf("review-classified correction changed the offset to %v", service.Offset())
	}
	if len(corrections) != 0 {
		t.Fatal("review-classified correction notified observers")
	}
	if monitor.PendingCount() != 1 {
		t.Fatalf("expected one pending review, got %d", monitor.PendingCount())
	}
	item, err := monitor.Decide(reviewID, true, "operator-1", "confirmed against UTC")
	if err != nil {
		t.Fatal(err)
	}
	if item.Status != stationclock.ReviewApproved || item.CorrectionID == "" {
		t.Fatalf("unexpected approved review: %#v", item)
	}
	if service.Offset() != 5*time.Second {
		t.Fatalf("approved correction was not applied, offset %v", service.Offset())
	}
	if len(corrections) != 1 {
		t.Fatalf("expected one observer notification after approval, got %d", len(corrections))
	}
}

func TestMonitorQueuesBackwardCorrectionAndRejects(t *testing.T) {
	service, monitor := newMonitorFixture(t)
	base := time.Now().UTC()
	for i := 0; i < 2; i++ {
		if _, err := monitor.Submit(sample("gps", 100*time.Millisecond, base.Add(time.Duration(i)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	if service.Offset() != 100*time.Millisecond {
		t.Fatalf("auto correction did not apply, offset %v", service.Offset())
	}
	var reviewID string
	for i := 0; i < 2; i++ {
		assessment, err := monitor.Submit(sample("gps", 40*time.Millisecond, base.Add(time.Duration(i+2)*time.Second)))
		if err != nil {
			t.Fatal(err)
		}
		reviewID = assessment.ReviewID
	}
	if reviewID == "" {
		t.Fatal("backward correction was not routed to manual review")
	}
	if service.Offset() != 100*time.Millisecond {
		t.Fatalf("backward review changed the offset to %v", service.Offset())
	}
	item, err := monitor.Decide(reviewID, false, "operator-2", "station keeps current offset")
	if err != nil {
		t.Fatal(err)
	}
	if item.Status != stationclock.ReviewRejected {
		t.Fatalf("unexpected rejected review: %#v", item)
	}
	if service.Offset() != 100*time.Millisecond {
		t.Fatalf("rejected correction changed the offset to %v", service.Offset())
	}
	if len(service.Corrections()) != 1 {
		t.Fatalf("rejected correction was persisted: %#v", service.Corrections())
	}
}

func TestMonitorExcludesSuspectSourceFromEnsemble(t *testing.T) {
	service, monitor := newMonitorFixture(t)
	base := time.Now().UTC()
	for i := 0; i < 2; i++ {
		at := base.Add(time.Duration(i) * time.Second)
		if _, err := monitor.Submit(sample("gps", 50*time.Millisecond, at)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		at := base.Add(time.Duration(i) * time.Second)
		if _, err := monitor.Submit(sample("ntp", 54*time.Millisecond, at)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		at := base.Add(time.Duration(i+2) * time.Second)
		if _, err := monitor.Submit(sample("rogue", 5*time.Second, at)); err != nil {
			t.Fatal(err)
		}
	}
	report := monitor.Quality()
	trusts := map[string]stationclock.Trust{}
	for _, stats := range report.Sources {
		trusts[stats.Source] = stats.Trust
	}
	if trusts["rogue"] != stationclock.TrustSuspect {
		t.Fatalf("rogue source was not marked suspect: %#v", trusts)
	}
	if trusts["gps"] != stationclock.TrustNominal || trusts["ntp"] != stationclock.TrustNominal {
		t.Fatalf("healthy sources lost trust: %#v", trusts)
	}
	if report.Ensemble != 52*time.Millisecond {
		t.Fatalf("suspect source dragged the ensemble to %v", report.Ensemble)
	}
	if service.Offset() != 52*time.Millisecond {
		t.Fatalf("suspect source moved the station offset to %v", service.Offset())
	}
}

func TestMonitorFlagsDriftingSourceDegraded(t *testing.T) {
	_, monitor := newMonitorFixture(t)
	base := time.Now().UTC()
	for i := 0; i < 3; i++ {
		offset := 50*time.Millisecond + time.Duration(i)*time.Millisecond
		if _, err := monitor.Submit(sample("gps", offset, base.Add(time.Duration(i)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err := monitor.Submit(sample("ntp", 51*time.Millisecond, base.Add(time.Duration(i)*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	report := monitor.Quality()
	var gps *stationclock.SourceStats
	for i := range report.Sources {
		if report.Sources[i].Source == "gps" {
			gps = &report.Sources[i]
		}
	}
	if gps == nil {
		t.Fatal("gps source missing from quality report")
	}
	if gps.DriftNsPerSec <= stationclock.DefaultPolicy().MaxDriftNsPerSec {
		t.Fatalf("expected drift above policy limit, got %v ns/s", gps.DriftNsPerSec)
	}
	if gps.Trust != stationclock.TrustDegraded {
		t.Fatalf("drifting source was not degraded: %#v", gps)
	}
}

func TestMonitorStateSurvivesReload(t *testing.T) {
	paths := store.NewPaths(t.TempDir())
	trail := audit.NewTrail(store.NewAppendLog(paths))
	service, err := stationclock.NewService(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	monitor, err := stationclock.NewMonitor(paths, service, trail, stationclock.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	var reviewID string
	for i := 0; i < 2; i++ {
		assessment, err := monitor.Submit(sample("gps", 5*time.Second, base.Add(time.Duration(i)*time.Second)))
		if err != nil {
			t.Fatal(err)
		}
		reviewID = assessment.ReviewID
	}
	reloaded, err := stationclock.NewMonitor(paths, service, trail, stationclock.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.PendingCount() != 1 {
		t.Fatal("pending review did not survive reload")
	}
	if _, err := reloaded.Decide(reviewID, false, "operator-3", "rejected after restart"); err != nil {
		t.Fatal(err)
	}
	if service.Offset() != 0 {
		t.Fatalf("rejected review changed the offset to %v", service.Offset())
	}
}
