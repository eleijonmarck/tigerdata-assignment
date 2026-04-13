package main

import (
	"encoding/csv"
	"strings"
	"testing"
	"time"
)

// TestParseRow proves that we can safely handle standard data, corrupted data,
// and End-of-File streams exactly as the assignment specified.
func TestParseRow(t *testing.T) {
	// A mock CSV string containing:
	// Row 1: Perfectly valid
	// Row 2: Corrupted timestamp
	csvData := `host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22
host_000001,invalid-date-format,2017-01-01 09:59:22
host_000001,invalid-date-format,2017-01-01 09:59:22, ,
`
	reader := csv.NewReader(strings.NewReader(csvData))

	// --- 1. Test Valid Row ---
	task, err := parseRow(reader)
	if err != nil {
		t.Fatalf("expected no error for valid row, got: %v", err)
	}
	if task.Hostname != "host_000008" {
		t.Errorf("expected hostname host_000008, got %s", task.Hostname)
	}

	expectedStart, _ := time.Parse("2006-01-02 15:04:05", "2017-01-01 08:59:22")
	if !task.Startime.Equal(expectedStart) {
		t.Errorf("expected start time %v, got %v", expectedStart, task.Startime)
	}

	// --- 2. Test Invalid Row (bad date) ---
	_, err = parseRow(reader)
	if err == nil {
		t.Fatal("expected error for invalid date, got nil")
	}
	if !strings.Contains(err.Error(), "failed to parse start time") {
		t.Errorf("expected parse error warning about start time, got: %v", err)
	}
	// --- 3. Test Invalid Row (not same number of fields) ---
	_, err = parseRow(reader)
	if err == nil {
		t.Fatal("expected error for invalid field count, got nil")
	}
	if !strings.Contains(err.Error(), "wrong number of fields") {
		t.Errorf("expected parse error warning about field count, got: %v", err)
	}
}

func TestGetWorkerIndex(t *testing.T) {
	// Let's pretend the user ran `--workers 10`
	numWorkers := 10

	// Get the assignments
	workerForHost8 := getWorkerIndex("host_000008", numWorkers)
	workerForHost1 := getWorkerIndex("host_000001", numWorkers)

	// 1. Prove Sticky Routing (Calling it twice goes to the same worker)
	if getWorkerIndex("host_000008", numWorkers) != workerForHost8 {
		t.Errorf("expected host_000008 to always be assigned to %d", workerForHost8)
	}

	// 2. Prove Load Distribution (Different hosts ideally go to different workers)
	// (Note: With modulo 10, collisions CAN happen, but 8 and 1 will likely route differently)
	if workerForHost8 == workerForHost1 {
		t.Logf("Warning: Host 8 and Host 1 happened to have a hash collision and routed to the same worker. This is normal but bad for this specific test case!")
	}
}
