# Data Loader POC

This is a proof of concept utilizing PostgreSQL/MySQL connection pooling along with Go's concurrency worker pool pattern.

It downloads a CSV dataset file from the internet, and loads one million records into DB.

### Instructions

Run a PostgreSQL/MySQL database locally either using Docker or not.

Create `.env` file from `.env.example`.

Run

```bash
go run ./cmd/pgloader/main.go
```

```bash
go run ./cmd/mysqlloader/main.go
```

[_crazyoptimist_](https://crazyoptimist.net)
