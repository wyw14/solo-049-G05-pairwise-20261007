package clock

import (
	"fmt"
	"sort"
	"time"
)

// Trust classifies how much weight a time source carries in the ensemble.
type Trust string

const (
	TrustUnknown  Trust = "unknown"
	TrustNominal  Trust = "trusted"
	TrustDegraded Trust = "degraded"
	TrustSuspect  Trust = "suspect"
)

// Policy bounds which corrections may apply automatically. Corrections beyond
// MaxAutoAdvance, and every backward step, are forced into manual review so an
// operator decides before any active window can move.
type Policy struct {
	MinCorrection     time.Duration `json:"min_correction"`
	MaxAutoAdvance    time.Duration `json:"max_auto_advance"`
	DegradedDeviation time.Duration `json:"degraded_deviation"`
	SuspectDeviation  time.Duration `json:"suspect_deviation"`
	MaxDriftNsPerSec  float64       `json:"max_drift_ns_per_sec"`
	MinSamples        int           `json:"min_samples"`
	History           int           `json:"history"`
}

func DefaultPolicy() Policy {
	return Policy{
		MinCorrection:     time.Millisecond,
		MaxAutoAdvance:    250 * time.Millisecond,
		DegradedDeviation: 25 * time.Millisecond,
		SuspectDeviation:  100 * time.Millisecond,
		MaxDriftNsPerSec:  10_000,
		MinSamples:        2,
		History:           16,
	}
}

func (p Policy) Validate() error {
	if p.MinCorrection < 0 || p.MaxAutoAdvance < p.MinCorrection {
		return fmt.Errorf("clock policy needs auto advance limit above the noise floor")
	}
	if p.DegradedDeviation <= 0 || p.SuspectDeviation < p.DegradedDeviation {
		return fmt.Errorf("clock policy deviation bounds are inconsistent")
	}
	if p.MaxDriftNsPerSec <= 0 || p.MinSamples < 1 || p.History < 2 {
		return fmt.Errorf("clock policy drift, sample and history bounds are inconsistent")
	}
	return nil
}

// Decision records what the monitor did with an assessed ensemble offset.
type Decision string

const (
	DecisionNone   Decision = "none"
	DecisionAuto   Decision = "auto"
	DecisionReview Decision = "review"
)

// SourceStats summarizes one time source: offset, drift and trust.
type SourceStats struct {
	Source        string        `json:"source"`
	Samples       int           `json:"samples"`
	LastOffset    time.Duration `json:"last_offset"`
	LastObserved  time.Time     `json:"last_observed"`
	DriftNsPerSec float64       `json:"drift_ns_per_sec"`
	Deviation     time.Duration `json:"deviation"`
	Trust         Trust         `json:"trust"`
}

// Assessment is the outcome of evaluating one submitted sample.
type Assessment struct {
	At           time.Time     `json:"at"`
	Ensemble     time.Duration `json:"ensemble"`
	Current      time.Duration `json:"current"`
	Delta        time.Duration `json:"delta"`
	Decision     Decision      `json:"decision"`
	Reason       string        `json:"reason"`
	Contributing []string      `json:"contributing"`
	CorrectionID string        `json:"correction_id,omitempty"`
	ReviewID     string        `json:"review_id,omitempty"`
}

func driftNsPerSec(samples []Sample) float64 {
	if len(samples) < 2 {
		return 0
	}
	first, last := samples[0], samples[len(samples)-1]
	span := last.ObservedAt.Sub(first.ObservedAt)
	if span <= 0 {
		return 0
	}
	return float64(last.Offset-first.Offset) / span.Seconds()
}

func medianDuration(values []time.Duration) time.Duration {
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
}

func absDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}

func absFloat(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}
