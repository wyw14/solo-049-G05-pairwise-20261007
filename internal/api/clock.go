package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	stationclock "orbitlink/internal/clock"
)

func (r *Runtime) submitClockSample(writer http.ResponseWriter, request *http.Request) {
	var input stationclock.Sample
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	assessment, err := r.ClockMonitor.Submit(input)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, assessment)
}

func (r *Runtime) clockQuality(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, r.ClockMonitor.Quality())
}

func (r *Runtime) listClockReviews(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": r.ClockMonitor.Reviews(request.URL.Query().Get("status"))})
}

func (r *Runtime) decideClockReview(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Approve   bool   `json:"approve"`
		DecidedBy string `json:"decided_by"`
		Note      string `json:"note"`
	}
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, err)
		return
	}
	item, err := r.ClockMonitor.Decide(chi.URLParam(request, "reviewID"), input.Approve, input.DecidedBy, input.Note)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, item)
}
