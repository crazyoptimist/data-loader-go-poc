package main

import (
	"log"

	"github.com/crazyoptimist/data-loader-go-poc/pkg/utils"
)

const (
	DATASET_URL       = "https://downloads.majestic.com/majestic_million.csv"
	DATASET_FILE_PATH = "./majestic_million.csv"
)

func main() {

	// Check if the dataset file is already downloaded
	fileExists, err := utils.FileExists(DATASET_FILE_PATH)
	if err != nil {
		log.Fatalln("Error while reading the existing dataset file: ", err)
	}

	// Download the dataset if not existing
	if fileExists == false {
		if err := utils.DownloadFile(DATASET_FILE_PATH, DATASET_URL); err != nil {
			log.Fatalln("Failed to download the dataset: ", err)
		}
	}

	log.Println("Download Completed")
}
