package clock

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

// TrustGrade ranks how much the station may act on the current time estimate.
type TrustGrade string

const (
	TrustLocked    TrustGrade = "locked"
	TrustUsable    TrustGrade = "usable"
	TrustDegraded  TrustGrade = "degraded"
	TrustUntrusted TrustGrade = "untrusted"
)

// Strategy is the disposition the quality engine assigns to an incoming sample.
type Strategy string

const (
	// StrategyApply is a small, trusted nudge: the civil offset is slewed immediately.
	StrategyApply Strategy = "apply"
	// StrategyReview is a large or backward step: a human must approve it.
	StrategyReview Strategy = "manual_review"
	// StrategyIgnore is a stale or untrusted report that changes nothing.
	StrategyIgnore Strategy = "ignored"
)

// Review status values.
const (
	ReviewPending   = "pending"
	ReviewApproved  = "approved"
	ReviewDismissed = "dismissed"
)

// ErrManualReviewRequired is returned when a correction must wait for an operator.
var ErrManualReviewRequired = errors.New("clock correction held for manual review")

// LargeStepThreshold bounds any correction that may be applied without a human.
// Steps at or beyond it are treated as large regardless of their direction.
const LargeStepThreshold = 250 * time.Millisecond

// MaxAcceptedOffset mirrors the station-wide civil-time safety limit in Service.Apply.
const MaxAcceptedOffset = 10 * time.Minute

// sourceWeights ranks reference sources used to combine multi-source samples.
var sourceWeights = map[string]float64{
	"gnss": 1.0, "gps": 0.95, "galileo": 0.9, "ntp": 0.6,
	"pps": 0.8, "irig": 0.7, "manual": 0.5,
}

// SourceQuality is the quality engine's intake record: one time report from one source.
type SourceQuality struct {
	Sample       Sample  `json:"sample"`
	Weight       float64 `json:"weight"`
	Contribution bool    `json:"contribution"`
	Ignored      bool    `json:"ignored"`
	Reason       string  `json:"reason,omitempty"`
}

// Estimate is the current multi-source picture of station time.
type Estimate struct {
	At          time.Time       `json:"at"`
	Offset      time.Duration   `json:"offset"`
	DriftPPM    float64         `json:"drift_ppm"`
	Spread      time.Duration   `json:"spread"`
	Trust       TrustGrade      `json:"trust"`
	SourceCount int             `json:"source_count"`
	Sources     []SourceQuality `json:"sources,omitempty"`
}

// Review queues a large or backward correction for an operator. It never moves
// the live offset until an operator approves it.
type Review struct {
	ID            string        `json:"id"`
	CreatedAt     time.Time     `json:"created_at"`
	DecidedAt     time.Time     `json:"decided_at,omitempty"`
	Status        string        `json:"status"`
	Reason        string        `json:"reason"`
	Sample        Sample        `json:"sample"`
	BeforeOffset  time.Duration `json:"before_offset"`
	ProposedAfter time.Duration `json:"proposed_after"`
	Delta         time.Duration `json:"delta"`
	Estimate      Estimate      `json:"estimate"`
	Backward      bool          `json:"backward"`
	Large         bool          `json:"large"`
	ReviewedBy    string        `json:"reviewed_by,omitempty"`
	DecisionNote  string        `json:"decision_note,omitempty"`
	CorrectionID  string        `json:"correction_id,omitempty"`
}

// DecisionResult is what Ingest returns to callers.
type DecisionResult struct {
	Strategy   Strategy    `json:"strategy"`
	Estimate   Estimate    `json:"estimate"`
	Correction *Correction `json:"correction,omitempty"`
	Review     *Review     `json:"review,omitempty"`
}

// ImpactItem describes how a proposed delta would touch one scheduled pass.
type ImpactItem struct {
	PassID         string    `json:"pass_id"`
	Phase          string    `json:"phase"`
	Active         bool      `json:"active"`
	BeforeCivilLOS time.Time `json:"before_civil_los"`
	AfterCivilLOS  time.Time `json:"after_civil_los"`
	Protected      bool      `json:"protected"`
}

// PointingImpact describes scheduled antenna pointing commands the delta would move.
type PointingImpact struct {
	CommandID string    `json:"command_id"`
	PassID    string    `json:"pass_id"`
	AntennaID string    `json:"antenna_id"`
	Revision  int64     `json:"revision"`
	Before    time.Time `json:"before_execute_at"`
	After     time.Time `json:"after_execute_at"`
	Retired   bool      `json:"retired"`
}

// ImpactReport binds a pending (or applied) correction to everything it could move.
type ImpactReport struct {
	ReviewID     string           `json:"review_id,omitempty"`
	Delta        time.Duration    `json:"delta"`
	Backward     bool             `json:"backward"`
	Passes       []ImpactItem     `json:"passes"`
	Pointing     []PointingImpact `json:"pointing"`
	AuditEventID string           `json:"audit_event_id,omitempty"`
}

// Deadlines is the slice of pass scheduling the engine needs.
type Deadlines interface {
	List() []DeadlineView
}

// DeadlineView is the read-only shape the engine consumes.
type DeadlineView struct {
	PassID   string
	Phase    string
	CivilLOS time.Time
	Active   bool
}

// PointingReader lists scheduled pointing commands.
type PointingReader interface {
	List(passID string) []PointingView
}

// PointingView is the read-only shape the engine consumes.
type PointingView struct {
	ID        string
	PassID    string
	AntennaID string
	Revision  int64
	ExecuteAt time.Time
	Retired   bool
}

// QualityService ingests multi-source time samples, scores clock trust, and
// gates every correction that could perturb a pass window.
type QualityService struct {
	mu           sync.RWMutex
	clock        *Service
	audit        *audit.Trail
	path         string
	maxAge       time.Duration
	sources      map[string][]Sample
	reviews      []Review
	drifts       map[string][]driftPoint
	deadlines    Deadlines
	pointing     PointingReader
	lastEstimate Estimate
}

type driftPoint struct {
	at     time.Time
	offset time.Duration
}

type candidate struct {
	sample Sample
	weight float64
}

// NewQualityService restores pending reviews and starts scoring the given clock.
func NewQualityService(paths store.Paths, clock *Service, trail *audit.Trail) (*QualityService, error) {
	quality := &QualityService{
		clock:   clock,
		audit:   trail,
		path:    paths.Snapshot("clock-reviews"),
		maxAge:  10 * time.Minute,
		sources: make(map[string][]Sample),
		drifts:  make(map[string][]driftPoint),
	}
	found, err := store.ReadJSON(quality.path, &quality.reviews)
	if err != nil {
		return nil, err
	}
	if !found {
		quality.reviews = []Review{}
	}
	clock.SetGate(quality)
	return quality, nil
}

// BindScheduling lets the runtime connect deadline and pointing state after the
// full dependency graph has been constructed.
func (q *QualityService) BindScheduling(deadlines Deadlines, pointing PointingReader) {
	q.mu.Lock()
	q.deadlines = deadlines
	q.pointing = pointing
	q.mu.Unlock()
}

// Ingest accepts one time sample, recomputes the ensemble estimate, and decides
// whether to slew the clock, ignore the report, or open a manual review.
func (q *QualityService) Ingest(sample Sample) (DecisionResult, error) {
	if sample.Source == "" || sample.ObservedAt.IsZero() {
		return DecisionResult{}, fmt.Errorf("clock sample source and observation time are required")
	}
	if sample.Offset > MaxAcceptedOffset || sample.Offset < -MaxAcceptedOffset {
		return DecisionResult{}, fmt.Errorf("clock offset exceeds station safety limit")
	}
	q.mu.Lock()
	current := q.clock.Offset()
	q.recordSampleLocked(sample)
	estimate := q.estimateLocked(sample.ObservedAt)
	q.lastEstimate = estimate
	delta := sample.Offset - current
	large := absDuration(delta) >= LargeStepThreshold
	backward := delta < 0
	switch {
	case estimate.SourceCount == 0 || estimate.Trust == TrustUntrusted:
		q.auditLocked("sample.untrusted", sample, estimate, map[string]any{"source": sample.Source, "offset_ns": sample.Offset.Nanoseconds()})
		q.mu.Unlock()
		return DecisionResult{Strategy: StrategyIgnore, Estimate: estimate}, nil
	case large || backward:
		review := Review{
			ID: uuid.NewString(), CreatedAt: time.Now().UTC(), Status: ReviewPending,
			Reason: reviewReason(large, backward), Sample: sample,
			BeforeOffset: current, ProposedAfter: sample.Offset, Delta: delta,
			Estimate: estimate, Backward: backward, Large: large,
		}
		q.reviews = append(q.reviews, review)
		if err := q.flushLocked(); err != nil {
			q.reviews = q.reviews[:len(q.reviews)-1]
			q.mu.Unlock()
			return DecisionResult{}, err
		}
		report := q.impactLocked(delta, review.ID)
		_, _ = q.audit.Record(audit.Event{
			Component: "clock", Action: "review.opened", Severity: audit.Warning, Subject: review.ID,
			Fields: map[string]any{
				"delta_ns": delta.Nanoseconds(), "source": sample.Source, "large": large,
				"backward": backward, "trust": string(estimate.Trust), "reason": review.Reason,
				"affected_passes": len(report.Passes), "protected_passes": countProtected(report.Passes),
			},
		})
		q.mu.Unlock()
		return DecisionResult{Strategy: StrategyReview, Estimate: estimate, Review: &review}, nil
	default:
		q.mu.Unlock()
		correction, err := q.clock.Apply(sample)
		if err != nil {
			return DecisionResult{}, err
		}
		q.mu.Lock()
		q.recordDriftLocked(sample)
		estimate = q.estimateLocked(sample.ObservedAt)
		q.lastEstimate = estimate
		q.auditLocked("correction.auto_slewed", sample, estimate, map[string]any{"correction_id": correction.ID, "delta_ns": correction.Delta.Nanoseconds()})
		q.mu.Unlock()
		return DecisionResult{Strategy: StrategyApply, Estimate: estimate, Correction: &correction}, nil
	}
}

// Approve applies a pending review after operator sign-off. The resulting
// correction flows through the normal clock observer path, so planned deadlines
// and pointing schedules move together; active windows stay monotonic.
func (q *QualityService) Approve(id, reviewer, note string) (Review, Correction, error) {
	q.mu.Lock()
	index, err := q.findPendingLocked(id)
	if err != nil {
		q.mu.Unlock()
		return Review{}, Correction{}, err
	}
	sample := q.reviews[index].Sample
	q.reviews[index].Status = ReviewApproved
	q.reviews[index].DecidedAt = time.Now().UTC()
	q.reviews[index].ReviewedBy = reviewer
	q.reviews[index].DecisionNote = note
	if err := q.flushLocked(); err != nil {
		q.mu.Unlock()
		return Review{}, Correction{}, err
	}
	review := q.reviews[index]
	q.mu.Unlock()

	correction, err := q.clock.applyReviewed(sample)
	if err != nil {
		q.markFailed(id, err)
		return Review{}, Correction{}, err
	}
	q.mu.Lock()
	q.recordDriftLocked(sample)
	for i := range q.reviews {
		if q.reviews[i].ID == id {
			q.reviews[i].CorrectionID = correction.ID
			review = q.reviews[i]
			break
		}
	}
	_ = q.flushLocked()
	report := q.impactLocked(correction.Delta, id)
	_, _ = q.audit.Record(audit.Event{
		Component: "clock", Action: "review.approved", Severity: audit.Warning, Subject: id,
		Fields: map[string]any{
			"reviewer": reviewer, "note": note, "correction_id": correction.ID,
			"delta_ns": correction.Delta.Nanoseconds(), "affected_passes": len(report.Passes),
			"protected_passes": countProtected(report.Passes), "pointing_commands": len(report.Pointing),
		},
	})
	q.mu.Unlock()
	return review, correction, nil
}

// Dismiss closes a review without moving the clock.
func (q *QualityService) Dismiss(id, reviewer, note string) (Review, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	index, err := q.findPendingLocked(id)
	if err != nil {
		return Review{}, err
	}
	q.reviews[index].Status = ReviewDismissed
	q.reviews[index].DecidedAt = time.Now().UTC()
	q.reviews[index].ReviewedBy = reviewer
	q.reviews[index].DecisionNote = note
	if err := q.flushLocked(); err != nil {
		return Review{}, err
	}
	review := q.reviews[index]
	_, _ = q.audit.Record(audit.Event{
		Component: "clock", Action: "review.dismissed", Severity: audit.Info, Subject: id,
		Fields: map[string]any{"reviewer": reviewer, "note": note, "delta_ns": review.Delta.Nanoseconds()},
	})
	return review, nil
}

// Impact computes what a pending review would change without applying it.
func (q *QualityService) Impact(id string) (ImpactReport, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	for _, review := range q.reviews {
		if review.ID == id {
			if review.Status != ReviewPending {
				return ImpactReport{}, fmt.Errorf("review %s is already %s", id, review.Status)
			}
			return q.impactLocked(review.Delta, id), nil
		}
	}
	return ImpactReport{}, fmt.Errorf("clock review %s not found", id)
}

// Reviews returns reviews, optionally filtered to pending ones.
func (q *QualityService) Reviews(pendingOnly bool) []Review {
	q.mu.RLock()
	defer q.mu.RUnlock()
	values := make([]Review, 0, len(q.reviews))
	for _, review := range q.reviews {
		if pendingOnly && review.Status != ReviewPending {
			continue
		}
		values = append(values, review)
	}
	return values
}

// Estimate returns the most recent ensemble estimate, recomputed from retained
// samples when no sample has arrived yet.
func (q *QualityService) Estimate() Estimate {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.lastEstimate.SourceCount > 0 {
		return q.lastEstimate
	}
	return q.estimateLocked(time.Now().UTC())
}

// AllowApply is the clock gate: direct callers of Service.Apply may only perform
// small forward slews. Anything large or backward goes through a review.
func (q *QualityService) AllowApply(before time.Duration, sample Sample) error {
	delta := sample.Offset - before
	if absDuration(delta) >= LargeStepThreshold {
		return fmt.Errorf("%w: %s step of %s exceeds %s", ErrManualReviewRequired,
			sizeWord(delta), delta, LargeStepThreshold)
	}
	if delta < 0 {
		return fmt.Errorf("%w: backward step of %s needs operator approval", ErrManualReviewRequired, delta)
	}
	return nil
}

func (q *QualityService) findPendingLocked(id string) (int, error) {
	for index := range q.reviews {
		if q.reviews[index].ID == id {
			if q.reviews[index].Status != ReviewPending {
				return -1, fmt.Errorf("review %s is already %s", id, q.reviews[index].Status)
			}
			return index, nil
		}
	}
	return -1, fmt.Errorf("clock review %s not found", id)
}

func (q *QualityService) markFailed(id string, cause error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := range q.reviews {
		if q.reviews[i].ID == id {
			q.reviews[i].Status = ReviewPending
			q.reviews[i].DecidedAt = time.Time{}
			q.reviews[i].DecisionNote = "application failed: " + cause.Error()
			_ = q.flushLocked()
			return
		}
	}
}

func (q *QualityService) recordSampleLocked(sample Sample) {
	values := q.sources[sample.Source]
	values = append(values, sample)
	cutoff := sample.ObservedAt.Add(-q.maxAge)
	pruned := values[:0]
	for _, value := range values {
		if !value.ObservedAt.Before(cutoff) {
			pruned = append(pruned, value)
		}
	}
	q.sources[sample.Source] = pruned
}

func (q *QualityService) recordDriftLocked(sample Sample) {
	points := append(q.drifts[sample.Source], driftPoint{at: sample.ObservedAt, offset: sample.Offset})
	if len(points) > 2 {
		points = points[len(points)-2:]
	}
	q.drifts[sample.Source] = points
}

// estimateLocked combines the freshest sample per source with source weights,
// sample-quality grades and source agreement.
func (q *QualityService) estimateLocked(now time.Time) Estimate {
	estimate := Estimate{At: now.UTC(), Trust: TrustUntrusted, Sources: []SourceQuality{}}
	var candidates []candidate
	cutoff := now.Add(-q.maxAge)
	for source, values := range q.sources {
		if len(values) == 0 {
			continue
		}
		fresh := values[len(values)-1]
		detail := SourceQuality{Sample: fresh, Weight: weightFor(source)}
		if fresh.ObservedAt.Before(cutoff) {
			detail.Ignored = true
			detail.Reason = "sample older than trust window"
			estimate.Sources = append(estimate.Sources, detail)
			continue
		}
		grade := qualityGrade(fresh.Quality)
		if grade <= 0 {
			detail.Ignored = true
			detail.Reason = "source reports unusable quality: " + fresh.Quality
			estimate.Sources = append(estimate.Sources, detail)
			continue
		}
		detail.Weight *= grade
		detail.Contribution = true
		estimate.Sources = append(estimate.Sources, detail)
		candidates = append(candidates, candidate{sample: fresh, weight: detail.Weight})
	}
	if len(candidates) == 0 {
		if len(q.sources) > 0 {
			estimate.Trust = TrustUntrusted
		}
		return estimate
	}
	var weightSum, offsetWeighted, minOff, maxOff float64
	for index, candidate := range candidates {
		weightSum += candidate.weight
		offsetWeighted += float64(candidate.sample.Offset) * candidate.weight
		if index == 0 {
			minOff, maxOff = float64(candidate.sample.Offset), float64(candidate.sample.Offset)
		}
		minOff = math.Min(minOff, float64(candidate.sample.Offset))
		maxOff = math.Max(maxOff, float64(candidate.sample.Offset))
	}
	estimate.Offset = time.Duration(math.Round(offsetWeighted / weightSum))
	estimate.Spread = time.Duration(maxOff - minOff)
	estimate.SourceCount = len(candidates)
	estimate.DriftPPM = q.driftPPMLocked()
	estimate.Trust = scoreTrust(len(candidates), len(q.sources), estimate.Spread, estimate.DriftPPM, candidates)
	return estimate
} // driftPPMLocked averages per-source drift between the last two accepted points.
// Drift is dimensionless: offset seconds elapsed per wall second, scaled to ppm.
func (q *QualityService) driftPPMLocked() float64 {
	var total float64
	var count int
	for _, points := range q.drifts {
		if len(points) < 2 {
			continue
		}
		elapsed := points[1].at.Sub(points[0].at).Seconds()
		if elapsed <= 0 {
			continue
		}
		rate := (points[1].offset - points[0].offset).Seconds() / elapsed
		total += rate * 1_000_000
		count++
	}
	if count == 0 {
		return 0
	}
	return total / float64(count)
}

func scoreTrust(contributing, total int, spread time.Duration, driftPPM float64, candidates []candidate) TrustGrade {
	agreement := true
	for _, candidate := range candidates {
		if absDuration(candidate.sample.Offset-candidates[0].sample.Offset) > 100*time.Millisecond {
			agreement = false
			break
		}
	}
	switch {
	case contributing >= 2 && agreement && spread < 100*time.Millisecond && math.Abs(driftPPM) < 50:
		return TrustLocked
	case contributing >= 1 && spread < 500*time.Millisecond && math.Abs(driftPPM) < 250:
		return TrustUsable
	case contributing >= 1 && spread < 2*time.Second:
		return TrustDegraded
	default:
		return TrustUntrusted
	}
}

// impactLocked projects a delta onto scheduled deadlines and pointing commands.
// Active (tracking/draining) passes are reported as protected and are not moved.
func (q *QualityService) impactLocked(delta time.Duration, reviewID string) ImpactReport {
	report := ImpactReport{ReviewID: reviewID, Delta: delta, Backward: delta < 0,
		Passes: []ImpactItem{}, Pointing: []PointingImpact{}}
	if q.deadlines != nil {
		for _, deadline := range q.deadlines.List() {
			item := ImpactItem{
				PassID: deadline.PassID, Phase: deadline.Phase, Active: deadline.Active,
				BeforeCivilLOS: deadline.CivilLOS, Protected: deadline.Active,
			}
			if !deadline.Active {
				item.AfterCivilLOS = deadline.CivilLOS.Add(delta)
			} else {
				item.AfterCivilLOS = deadline.CivilLOS
			}
			report.Passes = append(report.Passes, item)
		}
	}
	if q.pointing != nil {
		for _, command := range q.pointing.List("") {
			if command.Retired {
				continue
			}
			report.Pointing = append(report.Pointing, PointingImpact{
				CommandID: command.ID, PassID: command.PassID, AntennaID: command.AntennaID,
				Revision: command.Revision, Before: command.ExecuteAt,
				After: command.ExecuteAt.Add(delta),
			})
		}
	}
	return report
}

func (q *QualityService) flushLocked() error {
	return store.WriteJSON(q.path, q.reviews)
}

func (q *QualityService) auditLocked(action string, sample Sample, estimate Estimate, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["source"] = sample.Source
	fields["trust"] = string(estimate.Trust)
	fields["source_count"] = estimate.SourceCount
	fields["spread_ns"] = estimate.Spread.Nanoseconds()
	fields["drift_ppm"] = estimate.DriftPPM
	severity := audit.Info
	if estimate.Trust == TrustDegraded {
		severity = audit.Warning
	}
	if estimate.Trust == TrustUntrusted {
		severity = audit.Critical
	}
	_, _ = q.audit.Record(audit.Event{Component: "clock", Action: action, Severity: severity, Fields: fields})
}

func weightFor(source string) float64 {
	if weight, found := sourceWeights[source]; found {
		return weight
	}
	return 0.4
}

// qualityGrade maps a free-form sample quality string to a 0..1 multiplier.
func qualityGrade(quality string) float64 {
	switch quality {
	case "locked", "pps", "":
		return 1
	case "synced", "ok", "good":
		return 0.8
	case "coarse", "degraded", "holdover":
		return 0.4
	case "lost", "unlocked", "fault", "invalid":
		return 0
	default:
		return 0.5
	}
}

func reviewReason(large, backward bool) string {
	switch {
	case large && backward:
		return "large backward step"
	case large:
		return "step exceeds station auto-slew limit"
	default:
		return "backward step"
	}
}

func countProtected(items []ImpactItem) int {
	protected := 0
	for _, item := range items {
		if item.Protected {
			protected++
		}
	}
	return protected
}

func absDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}

func sizeWord(delta time.Duration) string {
	if absDuration(delta) >= LargeStepThreshold {
		return "large"
	}
	return "small"
}
