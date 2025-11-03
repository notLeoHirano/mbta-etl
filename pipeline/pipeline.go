package pipeline

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/notLeoHirano/mbta-etl/model"
)

// Easily readable types
type Vehicle = model.Vehicle
type Attributes = model.Attributes
type VehicleResponse = model.VehicleResponse
type VehicleRecord = model.VehicleRecord


// ETL Pipeline components
type ETLPipeline struct {
	apiURL string
	db     *sql.DB
}

func NewETLPipeline(apiURL string, dbPath string) (*ETLPipeline, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := initDatabase(db); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return &ETLPipeline{
		apiURL: apiURL,
		db:     db,
	}, nil
}

func initDatabase(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS vehicles (
		id TEXT PRIMARY KEY,
		label TEXT NOT NULL,
		latitude REAL NOT NULL,
		longitude REAL NOT NULL,
		speed REAL NOT NULL,
		direction_id INTEGER NOT NULL,
		current_status TEXT NOT NULL,
		occupancy_status TEXT NOT NULL,
		bearing INTEGER NOT NULL,
		updated_at TIMESTAMP NOT NULL,
		ingested_at TIMESTAMP NOT NULL
	);
	
	CREATE INDEX IF NOT EXISTS idx_updated_at ON vehicles(updated_at);
	CREATE INDEX IF NOT EXISTS idx_label ON vehicles(label);
	`

	_, err := db.Exec(schema)
	return err
}


// Run full pipeline
func (p *ETLPipeline) Run() error {
	// Extract
	log.Println("Extracting data from MBTA API...")
	vehicleResp, err := p.Extract()
	if err != nil {
		return fmt.Errorf("extract failed: %w", err)
	}
	log.Printf("Extracted %d vehicles", len(vehicleResp.Data))

	// Transform
	log.Println("Transforming data...")
	records, err := p.Transform(vehicleResp.Data)
	if err != nil {
		return fmt.Errorf("transform failed: %w", err)
	}
	log.Printf("Transformed %d records", len(records))

	// Load
	log.Println("Loading data to database...")
	if err := p.Load(records); err != nil {
		return fmt.Errorf("load failed: %w", err)
	}
	log.Printf("Successfully loaded %d records", len(records))

	return nil
}

func (p *ETLPipeline) Close() error {
	return p.db.Close()
}
