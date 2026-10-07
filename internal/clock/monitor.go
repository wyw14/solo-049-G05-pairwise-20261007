package clock

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

type ReviewStatus string

const (
	ReviewPending    ReviewStatus = "pending"
	ReviewApproved   ReviewStatus = "approved"
	ReviewRejected   ReviewStatus = "rejected"
	ReviewSuperseded ReviewStatus = "superseded"
)

// ReviewItem is a correction that may only take effect after an operator
// approves it. Until then it changes nothing: no offset, deadline or
// pointing state moves.
type ReviewItem struct {
	ID           string        `json:"id"`
	QueuedAt     time.Time     `json:"queued_at"`
	Proposed     time.Duration `json:"proposed"`
	Delta        time.Duration `json:"delta"`
	Reason       string        `json:"reason"`
	Contributing []string      `json:"contributing"`
	Status       ReviewStatus  `json:"status"`
	DecidedAt    time.Time     `json:"decided_at,omitempty"`
	DecidedBy    string        `json:"decided_by,omitempty"`
	Note         string        `json:"note,omitempty"`
	CorrectionID string        `json:"correction_id,omitempty"`
}

// QualityReport is the operator-facing view of station clock health.
type QualityReport struct {
	At             time.Time     `json:"at"`
	Offset         time.Duration `json:"offset"`
	Ensemble       time.Duration `json:"ensemble"`
	Sources        []SourceStats `json:"sources"`
	PendingReviews int           `json:"pending_reviews"`
	LastAssessment *Assessment   `json:"last_assessment,omitempty"`
}

type monitorState struct {
	Histories map[string][]Sample `json:"histories"`
	Trust     map[string]Trust    `json:"trust"`
}

// Monitor evaluates multi-source time samples, tracks per-source offset,
// drift and trust, and decides whether a correction may apply automatically
// or must wait for manual review.
type Monitor struct {
	mu         sync.RWMutex
	service    *Service
	audit      *audit.Trail
	policy     Policy
	histories  map[string][]Sample
	trust      map[string]Trust
	reviews    []ReviewItem
	statsPath  string
	reviewPath string
	last       *Assessment
}

func NewMonitor(paths store.Paths, service *Service, trail *audit.Trail, policy Policy) (*Monitor, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	monitor := &Monitor{
		service: service, audit: trail, policy: policy,
		histories: make(map[string][]Sample), trust: make(map[string]Trust),
		statsPath: paths.Snapshot("clock-quality"), reviewPath: paths.Snapshot("clock-reviews"),
	}
	var state monitorState
	found, err := store.ReadJSON(monitor.statsPath, &state)
	if err != nil {
		return nil, err
	}
	if found {
		monitor.histories = state.Histories
		monitor.trust = state.Trust
	}
	if _, err := store.ReadJSON(monitor.reviewPath, &monitor.reviews); err != nil {
		return nil, err
	}
	return monitor, nil
}

// Submit ingests one sample, re-evaluates source trust and the ensemble
// offset, then applies, queues or skips a correction according to policy.
func (m *Monitor) Submit(sample Sample) (Assessment, error) {
	if sample.Source == "" || sample.ObservedAt.IsZero() {
		return Assessment{}, fmt.Errorf("clock sample source and observation time are required")
	}
	if sample.Offset > 10*time.Minute || sample.Offset < -10*time.Minute {
		m.record(audit.Event{Component: "clock", Action: "sample.rejected", Severity: audit.Warning, Subject: sample.Source, Fields: map[string]any{"offset_ns": sample.Offset.Nanoseconds(), "reason": "station safety limit"}})
		return Assessment{}, fmt.Errorf("clock offset exceeds station safety limit")
	}
	m.mu.Lock()
	history := append(m.histories[sample.Source], sample)
	if len(history) > m.policy.History {
		history = history[len(history)-m.policy.History:]
	}
	m.histories[sample.Source] = history
	trustEvents := m.recomputeTrustLocked()
	ensemble, contributing := m.ensembleLocked()
	assessment := Assessment{
		At: time.Now().UTC(), Ensemble: ensemble, Current: m.service.Offset(),
		Contributing: contributing,
	}
	assessment.Delta = assessment.Ensemble - assessment.Current
	var persistReviews bool
	switch {
	case len(contributing) == 0:
		assessment.Decision = DecisionNone
		assessment.Reason = "no trusted time sources"
	case absDuration(assessment.Delta) < m.policy.MinCorrection:
		assessment.Decision = DecisionNone
		assessment.Reason = "ensemble within noise floor"
	case assessment.Delta >= 0 && assessment.Delta <= m.policy.MaxAutoAdvance:
		correction, err := m.service.Apply(Sample{ObservedAt: assessment.At, Offset: ensemble, Source: "ensemble:" + joinSources(contributing), Quality: "locked"})
		if err != nil {
			m.mu.Unlock()
			return Assessment{}, err
		}
		assessment.Decision = DecisionAuto
		assessment.Reason = "forward correction within auto limit"
		assessment.CorrectionID = correction.ID
	default:
		item := m.queueReviewLocked(assessment)
		assessment.Decision = DecisionReview
		assessment.Reason = item.Reason
		assessment.ReviewID = item.ID
		persistReviews = true
	}
	assessmentCopy := assessment
	m.last = &assessmentCopy
	statsErr := m.persistStatsLocked()
	var reviewsErr error
	if persistReviews {
		reviewsErr = m.persistReviewsLocked()
	}
	m.mu.Unlock()
	if statsErr != nil {
		return Assessment{}, statsErr
	}
	if reviewsErr != nil {
		return Assessment{}, reviewsErr
	}
	for _, event := range trustEvents {
		m.record(event)
	}
	severity := audit.Info
	if assessment.Decision == DecisionReview {
		severity = audit.Warning
	}
	m.record(audit.Event{Component: "clock", Action: "sample.evaluated", Severity: severity, Subject: sample.Source, Fields: map[string]any{
		"offset_ns": sample.Offset.Nanoseconds(), "ensemble_ns": assessment.Ensemble.Nanoseconds(),
		"delta_ns": assessment.Delta.Nanoseconds(), "decision": string(assessment.Decision), "reason": assessment.Reason,
		"correction_id": assessment.CorrectionID, "review_id": assessment.ReviewID,
	}})
	if assessment.Decision == DecisionReview {
		m.record(audit.Event{Component: "clock", Action: "review.queued", Severity: audit.Warning, Subject: assessment.ReviewID, Fields: map[string]any{
			"proposed_ns": assessment.Ensemble.Nanoseconds(), "delta_ns": assessment.Delta.Nanoseconds(), "reason": assessment.Reason,
		}})
	}
	return assessment, nil
}

// Decide resolves a pending review. Approval applies the proposed offset
// through the clock service, so deadlines, pointing and audit all observe a
// human-authorized correction; rejection discards it.
func (m *Monitor) Decide(reviewID string, approve bool, decidedBy, note string) (ReviewItem, error) {
	if decidedBy == "" {
		decidedBy = "operator"
	}
	m.mu.Lock()
	index := -1
	for i := range m.reviews {
		if m.reviews[i].ID == reviewID {
			index = i
			break
		}
	}
	if index < 0 {
		m.mu.Unlock()
		return ReviewItem{}, fmt.Errorf("clock review %s not found", reviewID)
	}
	item := m.reviews[index]
	if item.Status != ReviewPending {
		m.mu.Unlock()
		return ReviewItem{}, fmt.Errorf("clock review %s is already %s", reviewID, item.Status)
	}
	var correction Correction
	if approve {
		applied, err := m.service.Apply(Sample{ObservedAt: time.Now().UTC(), Offset: item.Proposed, Source: "review:" + item.ID, Quality: "locked"})
		if err != nil {
			m.mu.Unlock()
			return ReviewItem{}, err
		}
		correction = applied
		item.Status = ReviewApproved
		item.CorrectionID = correction.ID
	} else {
		item.Status = ReviewRejected
	}
	item.DecidedAt = time.Now().UTC()
	item.DecidedBy = decidedBy
	item.Note = note
	m.reviews[index] = item
	err := m.persistReviewsLocked()
	m.mu.Unlock()
	if err != nil {
		return ReviewItem{}, err
	}
	action := "review.rejected"
	severity := audit.Info
	if approve {
		action = "review.approved"
		severity = audit.Warning
	}
	m.record(audit.Event{Component: "clock", Action: action, Severity: severity, Subject: item.ID, Fields: map[string]any{
		"decided_by": decidedBy, "note": note, "proposed_ns": item.Proposed.Nanoseconds(), "correction_id": item.CorrectionID,
	}})
	return item, nil
}

// Reviews lists review items, optionally filtered by status.
func (m *Monitor) Reviews(status string) []ReviewItem {
	m.mu.RLock()
	defer m.mu.RUnlock()
	values := make([]ReviewItem, 0, len(m.reviews))
	for _, item := range m.reviews {
		if status == "" || string(item.Status) == status {
			values = append(values, item)
		}
	}
	return values
}

func (m *Monitor) PendingCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, item := range m.reviews {
		if item.Status == ReviewPending {
			count++
		}
	}
	return count
}

// Quality reports per-source offset, drift and trust plus the ensemble view.
func (m *Monitor) Quality() QualityReport {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ensemble, _ := m.ensembleLocked()
	report := QualityReport{At: time.Now().UTC(), Offset: m.service.Offset(), Ensemble: ensemble, Sources: m.statsLocked(ensemble)}
	for _, item := range m.reviews {
		if item.Status == ReviewPending {
			report.PendingReviews++
		}
	}
	if m.last != nil {
		last := *m.last
		report.LastAssessment = &last
	}
	return report
}

// queueReviewLocked supersedes any pending review and queues the new one.
func (m *Monitor) queueReviewLocked(assessment Assessment) ReviewItem {
	reason := "backward correction requires manual review"
	if assessment.Delta >= 0 {
		reason = "correction exceeds auto-apply limit"
	}
	item := ReviewItem{
		ID: uuid.NewString(), QueuedAt: assessment.At, Proposed: assessment.Ensemble,
		Delta: assessment.Delta, Reason: reason, Contributing: assessment.Contributing,
		Status: ReviewPending,
	}
	for i := range m.reviews {
		if m.reviews[i].Status == ReviewPending {
			m.reviews[i].Status = ReviewSuperseded
			m.reviews[i].DecidedAt = assessment.At
			m.reviews[i].DecidedBy = "superseded-by:" + item.ID
		}
	}
	m.reviews = append(m.reviews, item)
	return item
}

// recomputeTrustLocked updates every source's trust from its deviation
// against the ensemble and its drift, returning audit events for changes.
func (m *Monitor) recomputeTrustLocked() []audit.Event {
	ensemble, _ := m.ensembleLocked()
	events := make([]audit.Event, 0)
	for source, history := range m.histories {
		stats := statsFor(source, history, ensemble, m.policy)
		previous := m.trust[source]
		if previous == "" {
			previous = TrustUnknown
		}
		m.trust[source] = stats.Trust
		if stats.Trust != previous && previous != "" {
			severity := audit.Info
			if stats.Trust == TrustSuspect {
				severity = audit.Warning
			}
			events = append(events, audit.Event{Component: "clock", Action: "source.trust_changed", Severity: severity, Subject: source, Fields: map[string]any{
				"from": string(previous), "to": string(stats.Trust),
				"deviation_ns": stats.Deviation.Nanoseconds(), "drift_ns_per_sec": stats.DriftNsPerSec,
			}})
		}
	}
	return events
}

// ensembleLocked combines the latest offsets of usable sources. Proven
// sources (trusted or degraded) dominate; unproven ones only count while no
// proven source exists, so a fresh or wild source cannot drag an established
// ensemble.
func (m *Monitor) ensembleLocked() (time.Duration, []string) {
	proven := make([]time.Duration, 0, len(m.histories))
	unproven := make([]time.Duration, 0, len(m.histories))
	provenSources := make([]string, 0, len(m.histories))
	unprovenSources := make([]string, 0, len(m.histories))
	for source, history := range m.histories {
		if len(history) == 0 || m.trust[source] == TrustSuspect {
			continue
		}
		latest := history[len(history)-1].Offset
		if m.trust[source] == TrustNominal || m.trust[source] == TrustDegraded {
			proven = append(proven, latest)
			provenSources = append(provenSources, source)
		} else {
			unproven = append(unproven, latest)
			unprovenSources = append(unprovenSources, source)
		}
	}
	values, contributing := proven, provenSources
	if len(values) == 0 {
		values, contributing = unproven, unprovenSources
	}
	if len(values) == 0 {
		return 0, nil
	}
	sort.Strings(contributing)
	return medianDuration(values), contributing
}

func (m *Monitor) statsLocked(ensemble time.Duration) []SourceStats {
	stats := make([]SourceStats, 0, len(m.histories))
	for source, history := range m.histories {
		stats = append(stats, statsFor(source, history, ensemble, m.policy))
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].Source < stats[j].Source })
	return stats
}

func statsFor(source string, history []Sample, ensemble time.Duration, policy Policy) SourceStats {
	stats := SourceStats{Source: source, Samples: len(history), Trust: TrustUnknown}
	if len(history) == 0 {
		return stats
	}
	last := history[len(history)-1]
	stats.LastOffset = last.Offset
	stats.LastObserved = last.ObservedAt
	stats.DriftNsPerSec = driftNsPerSec(history)
	stats.Deviation = absDuration(last.Offset - ensemble)
	switch {
	case len(history) < policy.MinSamples:
		stats.Trust = TrustUnknown
	case stats.Deviation > policy.SuspectDeviation:
		stats.Trust = TrustSuspect
	case stats.Deviation > policy.DegradedDeviation || absFloat(stats.DriftNsPerSec) > policy.MaxDriftNsPerSec:
		stats.Trust = TrustDegraded
	default:
		stats.Trust = TrustNominal
	}
	return stats
}

func (m *Monitor) persistStatsLocked() error {
	return store.WriteJSON(m.statsPath, monitorState{Histories: m.histories, Trust: m.trust})
}

func (m *Monitor) persistReviewsLocked() error {
	return store.WriteJSON(m.reviewPath, m.reviews)
}

func (m *Monitor) record(event audit.Event) {
	_, _ = m.audit.Record(event)
}

func joinSources(sources []string) string {
	return strings.Join(sources, ",")
}
