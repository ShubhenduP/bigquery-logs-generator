package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"
)

type LogEntry struct {
	Timestamp time.Time `bigquery:"timestamp"`
	Message   string    `bigquery:"message"`
	Level     string    `bigquery:"level"`
	Instance  string    `bigquery:"instance"`
}

func main() {
	ctx := context.Background()

	// Create a BigQuery client using default credentials
	client, err := bigquery.NewClient(ctx, "qa-target")
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}
	defer client.Close()

	datasetID := "test_dataset"
	tableID := "logEntries"
	table := client.Dataset(datasetID).Table(tableID)

	// Create a channel to receive log entries
	logChannel := make(chan LogEntry)

	// Start a Go routine to process logs from the channel
	go func() {
		for logEntry := range logChannel {
			// Insert the log entry into BigQuery
			inserter := table.Inserter()
			if err := inserter.Put(ctx, logEntry); err != nil {
				log.Printf("Failed to insert log into BigQuery: %v", err)
			} else {
				fmt.Println("Log uploaded to BigQuery successfully!")
			}
		}
	}()
	// Simulate infinite log generation
	go generateLogs(logChannel)

	// Prevent the main function from exiting
	select {}
}

func generateLogs(logChannel chan LogEntry) {
	defer close(logChannel)

	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)
	instances := []string{"instance-1", "instance-2", "instance-3", "instance-4", "instance-5"}
	logLevels := []string{"INFO", "DEBUG", "WARN", "ERROR"}
	logMessages := []string{
		"User logged in: user_id=%d",
		"Database query executed: query=\"SELECT * FROM users WHERE user_id=%d\"",
		"Disk space running low: available_space=%dMB",
		"Failed to send email: user_id=%d, error=\"SMTP connection timeout\"",
		"User logged out: user_id=%d",
		"Cache refreshed: cache_name=\"user_session_cache\"",
		"New user registration: user_id=%d",
		"High memory usage: used_memory=%dMB",
		"Unable to connect to database: db_host=\"db.example.com\", error=\"Connection refused\"",
		"System rebooted: reason=\"Scheduled maintenance\"",
		"Configuration file loaded: file_path=\"/etc/app/config.yaml\"",
		"User password changed: user_id=%d",
		"CPU temperature high: temperature=%dC",
		"Application crashed: app_name=\"web_server\", error=\"Segmentation fault\"",
		"Service started: service_name=\"background_worker\"",
	}

	for {
		logLevel := logLevels[rng.Intn(len(logLevels))]
		logMessage := logMessages[rng.Intn(len(logMessages))]
		serviceInstance := instances[rng.Intn(len(instances))]

		userId := rng.Intn(10000)
		space := rng.Intn(5000) + 100
		usedMemory := rng.Intn(16000) + 1000
		temperature := rng.Intn(30) + 50

		formattedMessage := fmt.Sprintf(logMessage, userId, space, usedMemory, temperature)

		logEntry := LogEntry{
			Timestamp: time.Now(),
			Level:     logLevel,
			Message:   formattedMessage,
			Instance:  serviceInstance,
		}

		logChannel <- logEntry

		time.Sleep(time.Duration(rng.Intn(15)+1) * time.Second)
	}
}
