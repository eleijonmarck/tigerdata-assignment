package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
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

const (
	// Replace time.RFC3339 with the custom TimeLayout string "2006-01-02 15:04:05".
	TimeLayout = "2006-01-02 15:04:05"
	// the maxmin query for getting the cpu usage across each minute
	maxMinBucketQuery = `SELECT time_bucket('1 minute', ts) AS bucket, max(usage), min(usage) FROM cpu_usage WHERE host = $1 AND ts >= $2 AND ts <= $3 GROUP BY bucket;`
)

type QueryTask struct {
	Hostname string
	Startime time.Time
	Endtime  time.Time
}

type QueryResult struct {
	Duration time.Duration
}

func main() {
	cfg := loadConfig()

	workers := flag.Int("workers", 1, "number of concurrent workers")
	filePath := flag.String("file", "", "path to CSV file (default: stdin)")
	verbose := flag.Bool("verbose", false, "enable standard debug logs to stdout")
	flag.Parse()

	var logOutput io.Writer = io.Discard
	if *verbose {
		logOutput = os.Stdout
	}
	logger := slog.New(slog.NewJSONHandler(logOutput, nil))
	slog.SetDefault(logger)

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
		slog.Info("reading from file", "path", *filePath)
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

	// first attemptt with a single task queue
	// tasks := make(chan QueryTask, 100)
	// buffered channel, to wait before the workers are ready to receive tasks
	// Create an array/slice of channels
	workerChannels := make([]chan QueryTask, *workers)

	// NOTE: WE NEED TO CLEAR the buffered results channel so that we dont block workers
	// this happens if we have a buffered channel that is smaller than the number of rows and we dont clear it
	results := make(chan QueryResult, 100)

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		// Initialize the specific buffered channel for THIS worker
		workerChannels[i] = make(chan QueryTask, 100)
		wg.Add(1)
		// Give the worker its very own private channel
		go Worker(ctx, pool, workerChannels[i], results, &wg)
	}

	var finalDurations []time.Duration
	// We need a secondary WaitGroup JUST for the aggregator,
	// so main knows when the aggregator has finished reading the final dropped result
	var aggWg sync.WaitGroup
	aggWg.Add(1)
	go func() {
		defer aggWg.Done() // Signal when it finishes
		// This loop naturally breaks when main calls `close(results)`
		for res := range results {
			finalDurations = append(finalDurations, res.Duration)
		}
	}()

	for {
		t, err := parseRow(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			slog.Error("parsing row failed", "error", err)
			continue
		}
		workerIndex := getWorkerIndex(t.Hostname, *workers)

		// Send the task strictly to that worker's private channel
		workerChannels[workerIndex] <- t
	}

	// Replace your current `close(tasks)` with this:
	for i := 0; i < *workers; i++ {
		close(workerChannels[i])
	}

	slog.Info("waiting for workers")
	wg.Wait()
	slog.Info("workers finished")
	close(results)
	aggWg.Wait()
	slog.Info("results finished")

	prettyPrintResults(finalDurations)
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pool.Close()
	<-shutdownCtx.Done()

	slog.Info("shutdown complete")
}

func Worker(ctx context.Context, pool *pgxpool.Pool, queryTasks chan QueryTask, results chan QueryResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range queryTasks {
		start := time.Now()

		// execute query statement
		// Run the query!
		rows, err := pool.Query(ctx, maxMinBucketQuery, task.Hostname, task.Startime, task.Endtime)
		if err != nil {
			slog.Error("query failed", "error", err, "host", task.Hostname)
			continue
		}

		// constraint "capture the full round-trip... transfer results + read rows"
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
		results <- QueryResult{Duration: duration}
	}

}

// NOTE: improvement; make a "deadletter" queue by creating a file and log the messages/rows there
func parseRow(reader *csv.Reader) (QueryTask, error) {
	record, err := reader.Read()
	if err != nil {
		// If it's EOF, we just pass the EOF directly up so main knows to break
		if err == io.EOF {
			return QueryTask{}, io.EOF
		}
		if err == csv.ErrFieldCount {
			// should add to dead letter queue, or store somewhere
			return QueryTask{}, fmt.Errorf("csv field count error: %w (record: %v)", err, record)
		}
		// If it's a faulty row (wrong column count, etc), we throw a custom error
		return QueryTask{}, fmt.Errorf("faulty csv row: %w", err)
	}
	if len(record) != 3 {
		// expected 3 records from the query paramters
		return QueryTask{}, fmt.Errorf("wrong number of fields in csv, expected 3 got %d", len(record))
	}

	startTime, err := time.Parse(TimeLayout, record[1])
	if err != nil {
		return QueryTask{}, fmt.Errorf("failed to parse start time row %v: %w", record, err)
	}
	endTime, err := time.Parse(TimeLayout, record[2])
	if err != nil {
		return QueryTask{}, fmt.Errorf("failed to parse end time row %v: %w", record, err)
	}
	return QueryTask{
		Hostname: record[0],
		Startime: startTime,
		Endtime:  endTime,
	}, nil
}

func prettyPrintResults(results []time.Duration) {
	if len(results) == 0 {
		fmt.Println("No queries processed.")
		return
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i] < results[j]
	})

	numQueries := len(results)
	minQueryTime := results[0]
	maxQueryTime := results[numQueries-1]
	p95QueryTime := results[int(float64(len(results))*0.95)]

	var medianQueryTime time.Duration
	if numQueries%2 == 0 {
		medianQueryTime = (results[numQueries/2-1] + results[numQueries/2]) / 2
	} else {
		medianQueryTime = results[numQueries/2]
	}

	var totalTime time.Duration
	for _, d := range results {
		totalTime += d
	}
	avgQueryTime := totalTime / time.Duration(numQueries)

	type BenchmarkStats struct {
		NumQueries          int    `json:"num_queries"`
		TotalProcessingTime string `json:"total_processing_time"`
		MinQueryTime        string `json:"min_query_time"`
		MedianQueryTime     string `json:"median_query_time"`
		AvgQueryTime        string `json:"avg_query_time"`
		MaxQueryTime        string `json:"max_query_time"`
		P95QueryTime        string `json:"p95_query_time"`
	}

	stats := BenchmarkStats{
		NumQueries:          numQueries,
		TotalProcessingTime: totalTime.String(),
		MinQueryTime:        minQueryTime.String(),
		MedianQueryTime:     medianQueryTime.String(),
		AvgQueryTime:        avgQueryTime.String(),
		MaxQueryTime:        maxQueryTime.String(),
		P95QueryTime:        p95QueryTime.String(),
	}

	jsonData, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		slog.Error("failed to generate JSON", "error", err)
		return
	}
	fmt.Println(string(jsonData))
}

func hashHostname(hostname string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(hostname))
	return h.Sum32()
}

func getWorkerIndex(hostname string, numWorkers int) int {
	hashValue := hashHostname(hostname)
	return int(hashValue) % numWorkers
}
