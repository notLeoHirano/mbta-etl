package etl

import (
	"fmt"
	"log"
	"time"

	api "github.com/notLeoHirano/mbta-etl/mbta"
	"github.com/notLeoHirano/mbta-etl/transformer"
	"github.com/notLeoHirano/mbta-etl/vehiclestore" // Updated import
)

// Pipeline manages the end-to-end Extract-Transform-Load process.
type Pipeline struct {
	APIClient   api.Client
	Transformer transformer.Transformer
	Repo        vehiclestore.Repository // Updated interface type
}

// NewPipeline creates a new ETL pipeline instance with all dependencies.
func NewPipeline(client api.Client, xformer transformer.Transformer, repo vehiclestore.Repository) *Pipeline {
	return &Pipeline{
		APIClient:   client,
		Transformer: xformer,
		Repo:        repo,
	}
}

// Run executes the full ETL pipeline cycle.
func (p *Pipeline) Run() error {
	startTime := time.Now()
	log.Println("--- Starting ETL Cycle ---")

	// 1. Extract
	log.Println("1. Extracting data from MBTA API...")
	vehicleResp, err := p.APIClient.FetchVehicles()
	if err != nil {
		return fmt.Errorf("extract failed: %w", err)
	}
	log.Printf("   -> Extracted %d raw vehicles", len(vehicleResp.Data))

	// 2. Transform
	log.Println("2. Transforming and normalizing data...")
	records, err := p.Transformer.TransformVehicles(vehicleResp.Data)
	if err != nil {
		return fmt.Errorf("transform failed: %w", err)
	}
	log.Printf("   -> Transformed %d clean records", len(records))

	// 3. Load
	log.Println("3. Loading data to database...")
	if err := p.Repo.Load(records); err != nil {
		return fmt.Errorf("load failed: %w", err)
	}
	log.Printf("   -> Successfully loaded %d records", len(records))

	log.Printf("--- ETL Cycle Completed in %v ---\n", time.Since(startTime).Round(time.Millisecond))
	return nil
}
