package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"

	"github.com/crazyoptimist/data-loader-go-poc/internal/migrations"
	"github.com/crazyoptimist/data-loader-go-poc/pkg/utils"
)

const (
	DB_MAX_IDLE_CONN  = 4
	DB_MAX_CONN       = 100
	NUM_WORKER        = 100
	DATASET_URL       = "https://downloads.majestic.com/majestic_million.csv"
	DATASET_FILE_PATH = "./majestic_million.csv"
	// Only insert one million records from the CSV dataset
	NUM_RECORDS = 1e6
)

var (
	dataHeaders   []string
	dbConnString  string
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
		log.Fatal(err.Error())
	}

	dbHost := os.Getenv("DB_HOST")
	dbUsername := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	dbConnString = fmt.Sprintf("%s:%s@tcp(%s:3306)/%s", dbUsername, dbPassword, dbHost, dbName)

	start := time.Now()

	db, err := OpenDBConnection()
	if err != nil {
		log.Fatal(err.Error())
	}

	defer db.Close()

	RunMigration(db)

	file, err := os.Open(DATASET_FILE_PATH)
	if err != nil {
		log.Fatal("Error opening the CSV dataset =>", err.Error())
	}
	defer file.Close()

	csvReader := csv.NewReader(file)

	jobs := make(chan []interface{}, 0)
	var wg sync.WaitGroup

	go RunAllJobs(db, jobs, &wg)
	DispatchJobs(csvReader, jobs, &wg)

	wg.Wait()

	duration := time.Since(start)
	log.Printf("Done in %d seconds", int(math.Ceil(duration.Seconds())))
}

func OpenDBConnection() (*sql.DB, error) {

	db, err := sql.Open("mysql", dbConnString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(DB_MAX_CONN)
	db.SetMaxIdleConns(DB_MAX_IDLE_CONN)

	return db, nil
}

func RunMigration(db *sql.DB) {

	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Println("Error getting connection: ", err)
	}

	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), migrations.INIT_MAJESTIC_MILLIONS)
	if err != nil {
		log.Fatal("Error running migration =>\n", err.Error())
	}
}

func RunInsert(db *sql.DB, values []interface{}) {

	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Println("Error getting connection: ", err)
	}

	defer conn.Close()

	query := fmt.Sprintf("INSERT INTO domain (%s) VALUES (%s)",
		strings.Join(dataHeaders, ","),
		strings.Join(utils.GenerateMYSQLPlaceholder(len(dataHeaders)), ","),
	)

	_, err = conn.ExecContext(context.Background(), query, values...)
	if err != nil {
		log.Fatal("Error executing the query =>\n", err.Error())
	}

	mu.Lock()
	completedJobs++
	progress := float64(completedJobs) / float64(NUM_RECORDS) * 100
	mu.Unlock()

	utils.PrintProgress(&mu, progress)
}

func RunAllJobs(db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex <= NUM_WORKER; workerIndex++ {

		go func(db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {

			for job := range jobs {
				RunInsert(db, job)
				wg.Done()
			}

		}(db, jobs, wg)
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

		if len(dataHeaders) == 0 {
			dataHeaders = row
			continue
		}

		rowOrdered := make([]interface{}, len(row))
		for i, v := range row {
			rowOrdered[i] = v
		}

		wg.Add(1)
		jobs <- rowOrdered
	}
}
