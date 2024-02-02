package utils

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

// Downloads a remote file with a very small memory footprint
// using io.Copy
func DownloadFile(outputFilePath string, url string) (err error) {

	// Create the output file
	out, err := os.Create(outputFilePath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check the server response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// Write the body to the output file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}
