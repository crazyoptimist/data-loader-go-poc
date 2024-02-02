package utils

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func ConnectPostgres(connString string, maxConnNum int32) (*pgxpool.Pool, error) {

	dbConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}

	dbConfig.MaxConns = maxConnNum

	return pgxpool.NewWithConfig(context.Background(), dbConfig)
}

// Prints the current connection stats to stdout
func PrintPGConnStats(dbPool *pgxpool.Pool) {

	stats := dbPool.Stat()

	fmt.Printf("Total connections: %d\n", stats.TotalConns())
	fmt.Printf("Acquired connections: %d\n", stats.AcquiredConns())
	fmt.Printf("Idle connections: %d\n", stats.IdleConns())

}

// Generates a query placeholder string for pgx
// Parameters:
//
//	n: number of query arguments
//
// Returns:
//
//	query placeholder string
func GeneratePSQLPlaceholder(num int) (placeholder string) {

	for i := 1; i <= num; i++ {
		if placeholder == "" {
			placeholder = "$1"
			continue
		}
		placeholder = fmt.Sprintf("%s, $%d", placeholder, i)
	}
	return
}
