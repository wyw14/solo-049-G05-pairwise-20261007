package clock

import (
	"errors"
	"testing"
	"time"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

func newTestQuality(t *testing.T) (*QualityService, *Service) {
	t.Helper()
	paths := store.NewPaths(t.TempDir())
	trail := audit.NewTrail(store.NewAppendLog(paths))
	clock, err := NewService(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	quality, err := NewQualityService(paths, clock, trail)
	if err != nil {
		t.Fatal(err)
	}
	return quality, clock
}

func TestMultiSourceEnsembleLocksWithAgreement(t *testing.T) {
	quality, _ := newTestQuality(t)
	base := time.Now().UTC()
	if result, err := quality.Ingest(Sample{ObservedAt: base, Source: "gnss", Offset: 0, Quality: "locked"}); err != nil {
		t.Fatal(err)
	} else if result.Strategy != StrategyApply {
		t.Fatalf("first aligned sample should apply, got %s", result.Strategy)
	}
	result, err := quality.Ingest(Sample{ObservedAt: base.Add(time.Second), Source: "gps", Offset: 5 * time.Millisecond, Quality: "locked"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Estimate.Trust != TrustLocked {
		t.Fatalf("two agreeing sources should lock, got %s", result.Estimate.Trust)
	}
	if result.Estimate.SourceCount != 2 || result.Estimate.Spread != 5*time.Millisecond {
		t.Fatalf("unexpected ensemble: %#v", result.Estimate)
	}
	if result.Estimate.DriftPPM != 0 {
		t.Fatalf("identical offsets imply zero drift, got %f ppm", result.Estimate.DriftPPM)
	}
}

func TestDriftEstimateFromRepeatedSource(t *testing.T) {
	quality, _ := newTestQuality(t)
	base := time.Now().UTC()
	first := Sample{ObservedAt: base, Source: "gnss", Offset: 10 * time.Millisecond, Quality: "locked"}
	second := Sample{ObservedAt: base.Add(10 * time.Second), Source: "gnss", Offset: 20 * time.Millisecond, Quality: "locked"}
	for _, sample := range []Sample{first, second} {
		if _, err := quality.Ingest(sample); err != nil {
			t.Fatal(err)
		}
	}
	estimate := quality.Estimate()
	// 10 ms over 10 s = 1 ms/s = 1000 ppm.
	if estimate.DriftPPM < 999 || estimate.DriftPPM > 1001 {
		t.Fatalf("drift estimate %f ppm is not 1000 ppm", estimate.DriftPPM)
	}
	if estimate.Trust != TrustDegraded && estimate.Trust != TrustUntrusted {
		t.Fatalf("high drift should not read as healthy, got %s", estimate.Trust)
	}
}

func TestLargeCorrectionHeldForReviewAndApproved(t *testing.T) {
	quality, clock := newTestQuality(t)
	base := time.Now().UTC()
	if _, err := quality.Ingest(Sample{ObservedAt: base, Source: "gnss", Offset: 10 * time.Millisecond, Quality: "locked"}); err != nil {
		t.Fatal(err)
	}
	notifications := 0
	clock.Subscribe(func(Correction) { notifications++ })

	held, err := quality.Ingest(Sample{ObservedAt: base.Add(time.Second), Source: "gnss", Offset: 2 * time.Second, Quality: "locked"})
	if err != nil {
		t.Fatal(err)
	}
	if held.Strategy != StrategyReview || held.Review == nil || !held.Review.Large {
		t.Fatalf("large step must open a review: %#v", held)
	}
	if clock.Offset() != 10*time.Millisecond {
		t.Fatal("pending review must not move the live offset")
	}
	if notifications != 0 {
		t.Fatal("pending review must not notify deadline observers")
	}
	if _, _, err := quality.Approve(held.Review.ID, "operator", "gnss confirmed"); err != nil {
		t.Fatal(err)
	}
	if clock.Offset() != 2*time.Second {
		t.Fatalf("approved correction did not apply: %s", clock.Offset())
	}
	if notifications != 1 {
		t.Fatal("approval must flow through the normal correction observers")
	}
}

func TestBackwardCorrectionHeldAndDismissed(t *testing.T) {
	quality, clock := newTestQuality(t)
	base := time.Now().UTC()
	if _, err := quality.Ingest(Sample{ObservedAt: base, Source: "gnss", Offset: 100 * time.Millisecond, Quality: "locked"}); err != nil {
		t.Fatal(err)
	}
	held, err := quality.Ingest(Sample{ObservedAt: base.Add(time.Second), Source: "gnss", Offset: 50 * time.Millisecond, Quality: "locked"})
	if err != nil {
		t.Fatal(err)
	}
	if held.Strategy != StrategyReview || !held.Review.Backward || held.Review.Large {
		t.Fatalf("small backward step must open a backward review: %#v", held)
	}
	// Direct Service.Apply is gated the same way.
	if _, err := clock.Apply(Sample{ObservedAt: base.Add(2 * time.Second), Source: "gnss", Offset: 0}); !errors.Is(err, ErrManualReviewRequired) {
		t.Fatalf("direct apply must return ErrManualReviewRequired, got %v", err)
	}
	review, err := quality.Dismiss(held.Review.ID, "operator", "spurious report")
	if err != nil {
		t.Fatal(err)
	}
	if review.Status != ReviewDismissed || clock.Offset() != 100*time.Millisecond {
		t.Fatal("dismissed review must leave the offset untouched")
	}
	if _, _, err := quality.Approve(held.Review.ID, "operator", ""); err == nil {
		t.Fatal("decided review cannot be approved twice")
	}
}

func TestUntrustedSampleIsIgnored(t *testing.T) {
	quality, clock := newTestQuality(t)
	result, err := quality.Ingest(Sample{ObservedAt: time.Now().UTC(), Source: "ntp", Offset: time.Second, Quality: "lost"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Strategy != StrategyIgnore || result.Estimate.Trust != TrustUntrusted {
		t.Fatalf("lost-quality sample must be ignored: %#v", result)
	}
	if clock.Offset() != 0 {
		t.Fatal("ignored sample must not change the offset")
	}
}

func TestImpactProtectsActiveWindowAndShiftsPointing(t *testing.T) {
	quality, _ := newTestQuality(t)
	now := time.Now().UTC()
	quality.BindScheduling(fakeDeadlines{
		views: []DeadlineView{
			{PassID: "PASS-PLANNED", Phase: "planned", CivilLOS: now.Add(5 * time.Minute), Active: false},
			{PassID: "PASS-TRACKING", Phase: "tracking", CivilLOS: now.Add(time.Minute), Active: true},
		},
	}, fakePointing{views: []PointingView{
		{ID: "CMD-1", PassID: "PASS-PLANNED", AntennaID: "ANT-01", Revision: 1, ExecuteAt: now.Add(2 * time.Minute)},
	}})
	held, err := quality.Ingest(Sample{ObservedAt: now, Source: "gnss", Offset: 2 * time.Second, Quality: "locked"})
	if err != nil {
		t.Fatal(err)
	}
	report, err := quality.Impact(held.Review.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Passes) != 2 {
		t.Fatalf("impact should cover both passes, got %d", len(report.Passes))
	}
	for _, item := range report.Passes {
		switch item.PassID {
		case "PASS-PLANNED":
			if item.Protected || item.AfterCivilLOS.Sub(item.BeforeCivilLOS) != 2*time.Second {
				t.Fatalf("planned pass must be shifted: %#v", item)
			}
		case "PASS-TRACKING":
			if !item.Protected || item.AfterCivilLOS != item.BeforeCivilLOS {
				t.Fatalf("active pass window must not move: %#v", item)
			}
		}
	}
	if len(report.Pointing) != 1 || report.Pointing[0].After.Sub(report.Pointing[0].Before) != 2*time.Second {
		t.Fatalf("pointing command should move with the correction: %#v", report.Pointing)
	}
}

func TestReviewsPersistAcrossRestart(t *testing.T) {
	paths := store.NewPaths(t.TempDir())
	trail := audit.NewTrail(store.NewAppendLog(paths))
	clock, err := NewService(paths, trail)
	if err != nil {
		t.Fatal(err)
	}
	quality, err := NewQualityService(paths, clock, trail)
	if err != nil {
		t.Fatal(err)
	}
	held, err := quality.Ingest(Sample{ObservedAt: time.Now().UTC(), Source: "gnss", Offset: 2 * time.Second, Quality: "locked"})
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := NewQualityService(paths, clock, trail)
	if err != nil {
		t.Fatal(err)
	}
	pending := reloaded.Reviews(true)
	if len(pending) != 1 || pending[0].ID != held.Review.ID {
		t.Fatalf("pending review did not survive restart: %#v", pending)
	}
}

type fakeDeadlines struct{ views []DeadlineView }

func (f fakeDeadlines) List() []DeadlineView { return f.views }

type fakePointing struct{ views []PointingView }

func (f fakePointing) List(string) []PointingView { return f.views }
