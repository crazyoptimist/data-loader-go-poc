package utils

import (
	"fmt"
	"sync"
)

// Prints out the current progress percentage
// Parameters:
// mu *sync.Mutex
// progress: Current progress percentage
func PrintProgress(mu *sync.Mutex, progress float64) {
	mu.Lock()
	defer mu.Unlock()

	// Clear the current line
	fmt.Print("\r")

	// Print the progress
	fmt.Printf("Progress: %.2f%%", progress)
}
