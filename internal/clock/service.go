package clock

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/audit"
	"orbitlink/internal/store"
)

// Service tracks civil-time corrections while active durations remain monotonic.
type Service struct {
	mu          sync.RWMutex
	offset      time.Duration
	corrections []Correction
	observers   []Observer
	path        string
	audit       *audit.Trail
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

func (s *Service) MonotonicDeadline(duration time.Duration) time.Time {
	return time.Now().Add(duration)
}

// Offset reports the currently applied civil-time correction.
func (s *Service) Offset() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offset
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
		s.mu.Unlock()
		return Correction{}, err
	}
	observers := append([]Observer(nil), s.observers...)
	s.mu.Unlock()
	_, _ = s.audit.Record(audit.Event{Component: "clock", Action: "corrected", Subject: correction.ID, Fields: map[string]any{"delta_ns": correction.Delta.Nanoseconds(), "source": correction.Source}})
	for _, observer := range observers {
		observer(correction)
	}
	return correction, nil
}

func (s *Service) Corrections() []Correction {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Correction(nil), s.corrections...)
}
