package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	stationclock "orbitlink/internal/clock"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/scheduler"
)

// deadlineClockView adapts the pass deadline manager to the clock engine.
type deadlineClockView struct {
	deadlines *passsvc.DeadlineManager
}

func (v deadlineClockView) List() []stationclock.DeadlineView {
	values := v.deadlines.List()
	views := make([]stationclock.DeadlineView, 0, len(values))
	for _, value := range values {
		active := value.Phase == passsvc.Tracking || value.Phase == passsvc.Draining
		views = append(views, stationclock.DeadlineView{
			PassID: value.PassID, Phase: string(value.Phase), CivilLOS: value.CivilLOS, Active: active,
		})
	}
	return views
}

// pointingClockView adapts the pointing command queue to the clock engine.
type pointingClockView struct {
	queue *scheduler.CommandQueue
}

func (v pointingClockView) List(passID string) []stationclock.PointingView {
	commands := v.queue.List(passID)
	views := make([]stationclock.PointingView, 0, len(commands))
	for _, command := range commands {
		views = append(views, stationclock.PointingView{
			ID: command.ID, PassID: command.PassID, AntennaID: command.AntennaID,
			Revision: command.Revision, ExecuteAt: command.ExecuteAt, Retired: command.Retired,
		})
	}
	return views
}

func (r *Runtime) ingestClockSample(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		ObservedAt time.Time `json:"observed_at"`
		OffsetNS   int64     `json:"offset_ns"`
		Offset     string    `json:"offset"`
		Source     string    `json:"source"`
		Quality    string    `json:"quality"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	offset := time.Duration(input.OffsetNS)
	if input.Offset != "" {
		parsed, err := time.ParseDuration(input.Offset)
		if err != nil {
			writeError(writer, err)
			return
		}
		offset = parsed
	}
	observedAt := input.ObservedAt
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	result, err := r.ClockQuality.Ingest(stationclock.Sample{
		ObservedAt: observedAt, Offset: offset, Source: input.Source, Quality: input.Quality,
	})
	if err != nil {
		writeError(writer, err)
		return
	}
	status := http.StatusAccepted
	if result.Strategy == stationclock.StrategyApply {
		status = http.StatusOK
	}
	writeJSON(writer, status, result)
}

func (r *Runtime) clockQuality(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, r.ClockQuality.Estimate())
}

func (r *Runtime) listClockReviews(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{
		"items": r.ClockQuality.Reviews(request.URL.Query().Get("status") == "pending"),
	})
}

func (r *Runtime) clockReviewImpact(writer http.ResponseWriter, request *http.Request) {
	report, err := r.ClockQuality.Impact(chi.URLParam(request, "reviewID"))
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, report)
}

func (r *Runtime) approveClockReview(writer http.ResponseWriter, request *http.Request) {
	input := struct {
		Reviewer string `json:"reviewer"`
		Note     string `json:"note"`
	}{}
	_ = decodeJSON(request, &input)
	review, correction, err := r.ClockQuality.Approve(chi.URLParam(request, "reviewID"), input.Reviewer, input.Note)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"review": review, "correction": correction})
}

func (r *Runtime) dismissClockReview(writer http.ResponseWriter, request *http.Request) {
	input := struct {
		Reviewer string `json:"reviewer"`
		Note     string `json:"note"`
	}{}
	_ = decodeJSON(request, &input)
	review, err := r.ClockQuality.Dismiss(chi.URLParam(request, "reviewID"), input.Reviewer, input.Note)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, review)
}
