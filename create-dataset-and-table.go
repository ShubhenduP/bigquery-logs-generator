package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"log"
)

func CreateDatasetAndTable() {
	ctx := context.Background()

	// Set your Google Cloud project ID
	projectID := "qa-target"

	// Create a BigQuery client
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Create a dataset
	datasetID := "test_dataset"
	if err := createDataset(client, datasetID); err != nil {
		log.Fatalf("Failed to create dataset: %v", err)
	}

	// Define schema for the table
	schema := bigquery.Schema{
		&bigquery.FieldSchema{
			Name: "timestamp",
			Type: bigquery.TimestampFieldType,
		},
		&bigquery.FieldSchema{
			Name: "message",
			Type: bigquery.StringFieldType,
		},
		&bigquery.FieldSchema{
			Name: "level",
			Type: bigquery.StringFieldType,
		},
		&bigquery.FieldSchema{
			Name: "instance",
			Type: bigquery.StringFieldType,
		},
	}

	// Create a table
	tableID := "logEntries"
	if err := createTable(client, datasetID, tableID, schema); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	fmt.Println("Dataset and table created successfully")
}

// Function to create a dataset
func createDataset(client *bigquery.Client, datasetID string) error {
	ctx := context.Background()

	dataset := client.Dataset(datasetID)
	if err := dataset.Create(ctx, &bigquery.DatasetMetadata{
		Name: datasetID,
	}); err != nil {
		return fmt.Errorf("failed to create dataset: %v", err)
	}
	fmt.Printf("Dataset %s created.\n", datasetID)
	return nil
}

// Function to create a table
func createTable(client *bigquery.Client, datasetID, tableID string, schema bigquery.Schema) error {
	ctx := context.Background()

	table := client.Dataset(datasetID).Table(tableID)
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: schema,
	}); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}
	fmt.Printf("Table %s created in dataset %s.\n", tableID, datasetID)
	return nil
}
