# benchmark cli tool

A command line tool for benchmarking SELECT query performance across multiple **workers/clients** against a timescaledb instance.

Setup the timescaledb with migration

```bash
docker compose up -d
```

build the docker image

```bash
docker build -t benchmark-tool .
```

Example from assignment

```bash
docker run --network host benchmark-tool --file /path/to/csv --workers 10
```

STDIN

```bash
cat ./TimescaleDB_coding_assignment-RD_eng_setup/query_params.csv | docker run -i --network host benchmark-tool --workers 10
```

making sure that we can run the setup

```bash
docker compose down -v && docker compose up -d && sleep 10 && docker run --network host benchmark-tool --file query_params.csv --workers 10
```

### constraints considered

- The tool should gracefully handle files that are **larger** than the one given,
  and should not wait to start processing queries until all input is consumed.
- Each query should then be executed by one of the concurrent workers your tool creates, with
  the constraint that queries for the same hostname be executed by the same worker each time.
  - each worker can execute from multiple hostnames.
    - hostname -> worker mapping needed
- Handles any invalid input appropriately.
- You need to implement it so that the files potentially exceed the machine's memory capacity.
  > "total processing time" is that wall-clock time of the entire execution, or the aggregate duration of all queries?
  > Aggregate duration.
  > "query time" is it the timer start strictly at the database driver's execution call
  > It should capture the full round-trip (send query + execute + transfer results + read rows).

#### optional funcitonality

- Handle CSV as either STDIN or via a flag with the filename. :Check:
- Unit / functional tests. :check:
- Provides additional benchmark statistics that you think are interesting (be prepared to explain why, don’t just dump a bunch of numbers on the user). :check: (P95)

### approach taken

1. print all csv rows to stdout
1. execute query against all rows
1. made one generic worker pool
1. made a worker pool per hostname (hashing hostname to a worker pool)
1. made a worker to collect all of the query stats

### findings

needed to handle docker database healthchecks using pg_isready

needed to use csv.Reader, which reads one row in memory each time

needed to use a databasepool to not share a database connection and handle mutex.

needed to use a worker buffered channel bigger than the rows of the csv. otherwise the worker gets stuck to add into the buffered channel

needed to use a hashing function to map the hostnames to specific worker channels.

- Started using standard Go map map[string]int to remember which host belongs to which worker

needed to use an if len(results) == 0 check when printing stats.

### improvments that i want to make

- dead letter queues for handling bad rows
- online stats (meaning in process stats update from the queries), DO NOT keep in memory []time.Duration
- better dbpool to worker setup (meaning we should configure the dbpool per worker)
- obviously i would want a migration tool such as goose or similiar for db migrations
