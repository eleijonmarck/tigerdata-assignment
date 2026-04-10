package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// - parse flags: --workers, --file (or stdin)
// - open CSV reader (streaming, row by row)
// - create N workers
// - for each CSV row: hash hostname → pick worker channel → send job
// - close worker channels when CSV is done
// - collect all durations from results channel
// - call stats.Compute() + print

type config struct {
	connectionString string
}

func loadConfig() config {
	cs := os.Getenv("CONNECTION_STRING")
	if cs == "" {
		cs = "postgres://tsdbadmin:password@localhost:5432/homework?sslmode=disable"
	}
	return config{connectionString: cs}
}

type QueryTask struct {
	Hostname string
	Startime time.Time
	Endtime  time.Time
}

type QueryResult struct {
	queryTime time.Time
}

const maxMinBucketQuery = `SELECT time_bucket('1 minute', ts) AS bucket, max(usage), min(usage) FROM cpu_usage WHERE host = $1 AND ts >= $2 AND ts <= $3 GROUP BY bucket;`

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	cfg := loadConfig()

	workers := flag.Int("workers", 1, "number of concurrent workers")
	filePath := flag.String("file", "", "path to CSV file (default: stdin)")
	flag.Parse()

	// background task to know if we want to interrupt and handle any outstanding queries
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.connectionString)
	if err != nil {
		slog.Error("failed to create connection pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}

	slog.Info("benchmark started", "db", cfg.connectionString)

	var input io.Reader
	if *filePath == "" || *filePath == "-" {
		input = os.Stdin
	} else {
		// file parsing
		f, err := os.Open(*filePath)
		if err != nil {
			fmt.Printf("could not read file: %v", err)
			os.Exit(1)
		}
		defer f.Close()
		input = f
		fmt.Printf("reading from file: %s\n", *filePath)
	}
	// i could use the bufio.NewReader() <- but csv package already handles that for me,
	// by creating the buffer for me
	reader := csv.NewReader(input)

	// read header value
	_, err = reader.Read()
	if err != nil {
		fmt.Printf("failed to read header: %v\n", err)
		os.Exit(1)
	}

	// buffered channel, to wait before the workers are ready to receive tasks
	tasks := make(chan QueryTask, 100)
	results := make(chan QueryResult, 100)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err == csv.ErrFieldCount {
			// TODO: should add to dead letter queue, or store somewhere
			fmt.Printf("got error %e", err)
		}
		if err != nil {
			// persistant
			fmt.Printf("got error %e", err)
			fmt.Printf("record  %+v", record)
		}

		// Replace time.RFC3339 with the custom layout string "2006-01-02 15:04:05".
		const layout = "2006-01-02 15:04:05"
		// 2. Parse Start Time
		startTime, err := time.Parse(layout, record[1])
		if err != nil {
			fmt.Printf("failed to parse start time on row %v: %v\n", record, err)
			continue
		}
		// 3. Parse End Time
		endTime, err := time.Parse(layout, record[2])
		if err != nil {
			fmt.Printf("failed to parse end time on row %v: %v\n", record, err)
			continue
		}
		// TODO: add to task queue
		t := QueryTask{
			Hostname: record[0],
			Startime: startTime,
			Endtime:  endTime,
		}

		tasks <- t

		start := time.Now()

		// execute query statement
		// Run the query!
		rows, err := pool.Query(ctx, maxMinBucketQuery, t.Hostname, t.Startime, t.Endtime)
		if err != nil {
			slog.Error("query failed", "error", err, "host", t.Hostname)
			continue
		}
		defer rows.Close()

		// The requirement states "capture the full round-trip... transfer results + read rows"
		// So we must actually process the rows to complete the benchmarking definition.
		for rows.Next() {
			var bucket time.Time
			var maxUsage, minUsage float64
			if err := rows.Scan(&bucket, &maxUsage, &minUsage); err != nil {
				slog.Error("failed to scan row", "error", err)
			}
		}
		rows.Close() // ALWAYS close rows

		// Check for any errors that happened during iteration
		if err := rows.Err(); err != nil {
			slog.Error("error iterating rows", "error", err)
		}
		duration := time.Since(start)
		fmt.Printf("Query for %s took %v\n", t.Hostname, duration)
	}

	_ = workers

	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool.Close()
	<-shutdownCtx.Done()

	slog.Info("shutdown complete")
}

func Worker(queryTasks chan QueryTask, results chan QueryResult) {}
