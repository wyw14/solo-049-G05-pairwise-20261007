package clock

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

// Gate decides whether a proposed civil-time step may be applied directly.
// Implementations send large or backward steps to manual review instead.
type Gate interface {
	AllowApply(before time.Duration, sample Sample) error
}

// Service tracks civil-time corrections while active durations remain monotonic.
type Service struct {
	mu          sync.RWMutex
	offset      time.Duration
	corrections []Correction
	observers   []Observer
	path        string
	audit       *audit.Trail
	gate        Gate
}

// SetGate installs the quality gate that screens direct corrections.
func (s *Service) SetGate(gate Gate) {
	s.mu.Lock()
	s.gate = gate
	s.mu.Unlock()
}

func NewService(paths store.Paths, trail *audit.Trail) (*Service, error) {
	service := &Service{path: paths.Snapshot("clock"), audit: trail}
	var corrections []Correction
	found, err := store.ReadJSON(service.path, &corrections)
	if err != nil {
		return nil, err
	}
	if found {
		service.corrections = corrections
		if len(corrections) > 0 {
			service.offset = corrections[len(corrections)-1].After
		}
	}
	return service, nil
}

func (s *Service) Now() time.Time {
	s.mu.RLock()
	offset := s.offset
	s.mu.RUnlock()
	return time.Now().UTC().Add(offset)
}

// Offset returns the current civil-time correction.
func (s *Service) Offset() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offset
}

func (s *Service) MonotonicDeadline(duration time.Duration) time.Time {
	return time.Now().Add(duration)
}

func (s *Service) Subscribe(observer Observer) {
	s.mu.Lock()
	s.observers = append(s.observers, observer)
	s.mu.Unlock()
}

func (s *Service) Apply(sample Sample) (Correction, error) {
	if sample.Source == "" || sample.ObservedAt.IsZero() {
		return Correction{}, fmt.Errorf("clock sample source and observation time are required")
	}
	if sample.Offset > 10*time.Minute || sample.Offset < -10*time.Minute {
		return Correction{}, fmt.Errorf("clock offset exceeds station safety limit")
	}
	s.mu.Lock()
	if s.gate != nil {
		if err := s.gate.AllowApply(s.offset, sample); err != nil {
			s.mu.Unlock()
			return Correction{}, err
		}
	}
	correction, err := s.commitLocked(sample)
	s.mu.Unlock()
	if err != nil {
		return Correction{}, err
	}
	s.notify(correction)
	return correction, nil
}

// applyReviewed commits a correction an operator has already approved,
// bypassing the gate. The observer notification still protects active windows.
func (s *Service) applyReviewed(sample Sample) (Correction, error) {
	s.mu.Lock()
	correction, err := s.commitLocked(sample)
	s.mu.Unlock()
	if err != nil {
		return Correction{}, err
	}
	s.notify(correction)
	return correction, nil
}

// commitLocked appends and persists the correction. Caller holds s.mu.
func (s *Service) commitLocked(sample Sample) (Correction, error) {
	correction := Correction{
		ID: uuid.NewString(), AppliedAt: time.Now().UTC(), Delta: sample.Offset - s.offset,
		Before: s.offset, After: sample.Offset, Source: sample.Source,
	}
	previous := s.offset
	s.offset = sample.Offset
	s.corrections = append(s.corrections, correction)
	if err := store.WriteJSON(s.path, s.corrections); err != nil {
		s.offset = previous
		s.corrections = s.corrections[:len(s.corrections)-1]
		return Correction{}, err
	}
	return correction, nil
}

func (s *Service) notify(correction Correction) {
	s.mu.RLock()
	observers := append([]Observer(nil), s.observers...)
	s.mu.RUnlock()
	_, _ = s.audit.Record(audit.Event{Component: "clock", Action: "corrected", Subject: correction.ID, Fields: map[string]any{"delta_ns": correction.Delta.Nanoseconds(), "source": correction.Source}})
	for _, observer := range observers {
		observer(correction)
	}
}

func (s *Service) Corrections() []Correction {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Correction(nil), s.corrections...)
}
