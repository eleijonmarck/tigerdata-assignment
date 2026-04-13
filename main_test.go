package main

import (
	"encoding/csv"
	"strings"
	"testing"
	"time"
)

// TestParseRow proves that we can safely handle standard data, corrupted data
func TestParseRow(t *testing.T) {
	// row1: valid
	// row2: not valid timestamp
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
	numWorkers := 10

	workerForHost8 := getWorkerIndex("host_000008", numWorkers)

	if getWorkerIndex("host_000008", numWorkers) != workerForHost8 {
		t.Errorf("expected host_000008 to always be assigned to %d", workerForHost8)
	}
}
