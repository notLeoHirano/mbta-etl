package pipeline

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

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

// Extract: Fetch data from MBTA API
func (p *ETLPipeline) Extract() (*VehicleResponse, error) {
	resp, err := http.Get(p.apiURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var vehicleResp VehicleResponse
	if err := json.Unmarshal(body, &vehicleResp); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return &vehicleResp, nil
}

// Transform: Clean and normalize data
func (p *ETLPipeline) Transform(vehicles []Vehicle) ([]VehicleRecord, error) {
	records := make([]VehicleRecord, 0, len(vehicles))
	now := time.Now()

	for _, v := range vehicles {
		// Skip invalid records
		if v.ID == "" || v.Attributes.Label == "" {
			continue
		}

		// Parse timestamp
		updatedAt, err := time.Parse(time.RFC3339, v.Attributes.UpdatedAt)
		if err != nil {
			log.Printf("Warning: failed to parse timestamp for vehicle %s: %v", v.ID, err)
			updatedAt = now
		}

		// Handle nullable fields with defaults
		speed := 0.0
		if v.Attributes.Speed != nil {
			speed = *v.Attributes.Speed
		}

		bearing := 0
		if v.Attributes.Bearing != nil {
			bearing = *v.Attributes.Bearing
		}

		// Normalize status fields
		currentStatus := normalizeStatus(v.Attributes.CurrentStatus)
		occupancyStatus := normalizeStatus(v.Attributes.OccupancyStatus)

		record := VehicleRecord{
			ID:              v.ID,
			Label:           v.Attributes.Label,
			Latitude:        v.Attributes.Latitude,
			Longitude:       v.Attributes.Longitude,
			Speed:           speed,
			DirectionID:     v.Attributes.DirectionID,
			CurrentStatus:   currentStatus,
			OccupancyStatus: occupancyStatus,
			Bearing:         bearing,
			UpdatedAt:       updatedAt,
			IngestedAt:      now,
		}

		records = append(records, record)
	}

	return records, nil
}

// normalizeStatus ensures status fields are consistent
func normalizeStatus(status string) string {
	if status == "" {
		return "UNKNOWN"
	}
	return status
}

// Load: Store data in SQLite
func (p *ETLPipeline) Load(records []VehicleRecord) error {
	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO vehicles 
		(id, label, latitude, longitude, speed, direction_id, current_status, occupancy_status, bearing, updated_at, ingested_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, r := range records {
		_, err := stmt.Exec(
			r.ID, r.Label, r.Latitude, r.Longitude, r.Speed,
			r.DirectionID, r.CurrentStatus, r.OccupancyStatus,
			r.Bearing, r.UpdatedAt, r.IngestedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert record %s: %w", r.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
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

// functions for testing

// CountVehicles returns the total number of records in the vehicles table.
func (p *ETLPipeline) CountVehicles() (int, error) {
	var count int
	err := p.db.QueryRow("SELECT COUNT(*) FROM vehicles").Scan(&count)
	return count, err
}

// GetVehicleSpeed returns the speed of a vehicle by its ID.
func (p *ETLPipeline) GetVehicleSpeed(id string) (float64, error) {
	var speed float64
	err := p.db.QueryRow("SELECT speed FROM vehicles WHERE id = ?", id).Scan(&speed)
	return speed, err
}
