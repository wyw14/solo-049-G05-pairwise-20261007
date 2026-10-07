package scheduler

import (
	"testing"
	"time"

	"orbitlink/internal/store"
)

func TestCommandQueueShiftCivilSkipsRetiredAndActive(t *testing.T) {
	paths := store.NewPaths(t.TempDir())
	queue, err := NewCommandQueue(paths)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	planned := []PointingCommand{
		{PassID: "PASS-PLAN", ExecuteAt: now.Add(time.Minute), Azimuth: 10, Elevation: 10},
	}
	if err := queue.Replace("PASS-PLAN", 1, planned); err != nil {
		t.Fatal(err)
	}
	tracking := []PointingCommand{
		{PassID: "PASS-LIVE", ExecuteAt: now.Add(2 * time.Minute), Azimuth: 20, Elevation: 20},
	}
	if err := queue.Replace("PASS-LIVE", 1, tracking); err != nil {
		t.Fatal(err)
	}
	// A newer revision retires the planned pass's old commands.
	if err := queue.Replace("PASS-PLAN", 2, []PointingCommand{
		{PassID: "PASS-PLAN", ExecuteAt: now.Add(90 * time.Second), Azimuth: 11, Elevation: 11},
	}); err != nil {
		t.Fatal(err)
	}

	delta := 2 * time.Second
	shifted, err := queue.ShiftCivil(delta, map[string]bool{"PASS-LIVE": true})
	if err != nil || shifted != 1 {
		t.Fatalf("expected exactly one live planned command shifted, got %d, err=%v", shifted, err)
	}
	for _, command := range queue.List("") {
		switch command.PassID {
		case "PASS-PLAN":
			if !command.Retired && !command.ExecuteAt.Equal(now.Add(90*time.Second).Add(delta)) {
				t.Fatalf("live planned command should move by %s: %s", delta, command.ExecuteAt)
			}
		case "PASS-LIVE":
			if !command.ExecuteAt.Equal(now.Add(2 * time.Minute)) {
				t.Fatalf("active pass pointing must stay on monotonic time: %s", command.ExecuteAt)
			}
		}
	}

	// Shift must survive a restart.
	reloaded, err := NewCommandQueue(paths)
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range reloaded.List("PASS-PLAN") {
		if !command.Retired && !command.ExecuteAt.Equal(now.Add(90*time.Second).Add(delta)) {
			t.Fatalf("shifted timestamp was not persisted: %s", command.ExecuteAt)
		}
	}
}
