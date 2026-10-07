package console

import (
	"encoding/json"
	"net/http"
	"time"
)

type HealthSource interface {
	Health() Health
}

type Health struct {
	Status     string            `json:"status"`
	Service    string            `json:"service"`
	Now        time.Time         `json:"now"`
	StateRoot  string            `json:"state_root"`
	Components map[string]string `json:"components"`
	ClockTrust string            `json:"clock_trust"`
	Passes     int               `json:"passes"`
	Antennas   int               `json:"antennas"`
	RFChains   int               `json:"rf_chains"`
	Recordings int               `json:"recordings"`
}

func HealthHandler(source HealthSource) http.HandlerFunc {
	return func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(source.Health())
	}
}
