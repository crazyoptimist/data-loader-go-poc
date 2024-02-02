package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"github.com/crazyoptimist/data-loader-go-poc/internal/migrations"
	"github.com/crazyoptimist/data-loader-go-poc/pkg/utils"
)

const (
	DATASET_URL       = "https://downloads.majestic.com/majestic_million.csv"
	DATASET_FILE_PATH = "./majestic_million.csv"
	// PostgreSQL has a default limit of 115 concurrent connections,
	// with 15 reserved for superusers and 100 available for regular users
	DB_MAX_CONN = 100
	// This is less than max conn because,
	// we have some other connections like pgadmin, migration
	NUM_WORKERS = 95
	// Load only one million records from the dataset
	NUM_RECORDS = 1e6
)

var (
	dataHeaders   []string
	mu            sync.Mutex
	completedJobs int
)

func main() {

	// Check if the dataset file is already downloaded
	fileExists, err := utils.FileExists(DATASET_FILE_PATH)
	if err != nil {
		log.Fatalln("Error while reading the existing dataset file: ", err)
	}

	// Download the dataset if not existing
	if fileExists == false {
		err := utils.DownloadFile(DATASET_FILE_PATH, DATASET_URL)
		if err != nil {
			log.Fatalln("Failed to download the dataset: ", err)
		}
	}

	log.Println("Download Completed")

	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	// Connect to the Database
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbHost := os.Getenv("DB_HOST")
	dbName := os.Getenv("DB_NAME")

	dbUrl := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", dbUser, dbPassword, dbHost, dbName)

	dbPool, err := utils.ConnectPostgres(dbUrl, DB_MAX_CONN)
	if err != nil {
		log.Fatalln("Unable to create connection pool: ", err)
	}
	defer dbPool.Close()

	// Run migration
	if _, err := dbPool.Exec(context.Background(), migrations.INIT_MAJESTIC_MILLIONS); err != nil {
		log.Fatalln("Error while running migration: ", err)
	}

	// Open the CSV dataset file
	file, err := os.Open(DATASET_FILE_PATH)
	if err != nil {
		log.Fatalln("Error opening CSV file: ", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Start concurrently writing to the DB
	start := time.Now()

	jobs := make(chan []interface{}, 0)
	var wg sync.WaitGroup

	go RunAllJobs(dbPool, jobs, &wg)
	DispatchJobs(reader, jobs, &wg)

	wg.Wait()

	timeTaken := time.Since(start)
	log.Printf("Done in %d seconds", int(math.Ceil(timeTaken.Seconds())))
}

func RunInsert(pool *pgxpool.Pool, values []interface{}) {

	query := fmt.Sprintf("INSERT INTO domain (%s) VALUES (%s);",
		strings.Join(dataHeaders, ","),
		utils.GeneratePSQLPlaceholder(len(values)),
	)

	_, err := pool.Exec(context.Background(), query, values...)
	if err != nil {
		log.Fatalln("Error while running INSERT query", err)
	}

	mu.Lock()
	completedJobs++
	progress := float64(completedJobs) / float64(NUM_RECORDS) * 100
	mu.Unlock()

	utils.PrintProgress(&mu, progress)
}

func RunAllJobs(pool *pgxpool.Pool, jobs <-chan []interface{}, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex <= NUM_WORKERS; workerIndex++ {

		go func(pool *pgxpool.Pool, jobs <-chan []interface{}, wg *sync.WaitGroup) {

			for job := range jobs {
				RunInsert(pool, job)
				wg.Done()
			}

		}(pool, jobs, wg)
	}
}

func DispatchJobs(reader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	defer close(jobs)

	for i := 0; i < NUM_RECORDS; i++ {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalln("Error while reading the CSV file: ", err)
		}

		// First row as column names
		if len(dataHeaders) == 0 {
			dataHeaders = row
			continue
		}

		untypedRow := make([]interface{}, len(row))
		for i, v := range row {
			untypedRow[i] = v
		}

		wg.Add(1)
		jobs <- untypedRow
	}
}
