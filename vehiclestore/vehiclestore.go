package vehiclestore

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/notLeoHirano/mbta-etl/model"
	_ "modernc.org/sqlite" // SQLite driver
)

// Repository defines the interface for data persistence and retrieval.
type Repository interface {
	Init() error
	Load(records []model.VehicleRecord) error
	Close() error
	GetTop10FastestVehicles() ([]model.VehicleRecord, error)
	GetRouteBreakdown() ([]model.QueryStat, error)
	GetSummaryStats() (model.QueryStat, error)
}

// VehicleStore handles all SQLite interactions for MBTA vehicle data.
type VehicleStore struct {
	db *sql.DB
}

func NewVehicleStore(dbPath string) (*VehicleStore, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	repo := &VehicleStore{db: db}
	if err := repo.Init(); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return repo, nil
}

// Init creates the necessary tables and indexes.
func (r *VehicleStore) Init() error {
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
	_, err := r.db.Exec(schema)
	return err
}

// Load performs a bulk UPSERT operation within a transaction.
func (r *VehicleStore) Load(records []model.VehicleRecord) error {
	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Use INSERT OR REPLACE for UPSERT functionality
	stmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO vehicles 
		(id, label, latitude, longitude, speed, direction_id, current_status, occupancy_status, bearing, updated_at, ingested_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, record := range records {
		_, err := stmt.Exec(
			record.ID, record.Label, record.Latitude, record.Longitude, record.Speed,
			record.DirectionID, record.CurrentStatus, record.OccupancyStatus,
			record.Bearing, record.UpdatedAt.Format(time.RFC3339), record.IngestedAt.Format(time.RFC3339),
		)
		if err != nil {
			return fmt.Errorf("failed to insert record %s: %w", record.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetTop10FastestVehicles retrieves the 10 fastest vehicles.
func (r *VehicleStore) GetTop10FastestVehicles() ([]model.VehicleRecord, error) {
	query := `
		SELECT id, label, latitude, longitude, speed, direction_id, current_status, occupancy_status, bearing, updated_at, ingested_at
		FROM vehicles
		ORDER BY speed DESC
		LIMIT 10
	`
	return r.queryVehicles(query)
}

// GetRouteBreakdown provides summary statistics broken down by route type inferred from ID prefix.
func (r *VehicleStore) GetRouteBreakdown() ([]model.QueryStat, error) {
	query := `
		SELECT 
			CASE 
				WHEN id LIKE 'R-%' THEN 'Red Line'
				WHEN id LIKE 'O-%' THEN 'Orange Line'
				WHEN id LIKE 'G-%' THEN 'Green Line'
				WHEN id LIKE 'B-%' THEN 'Blue Line'
				WHEN id LIKE 'y%' THEN 'Bus'
				WHEN id LIKE 'ynk%' THEN 'Commuter Rail'
				ELSE 'Other'
			END as route_type,
			COUNT(*) as count,
			AVG(speed) as avg_speed,
			MAX(speed) as max_speed
		FROM vehicles
		GROUP BY route_type
		ORDER BY count DESC
	`
	
	rows, err := r.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []model.QueryStat
	for rows.Next() {
		var routeType string
		var count int
		var avgSpeed, maxSpeed sql.NullFloat64 
		
		err := rows.Scan(&routeType, &count, &avgSpeed, &maxSpeed)
		if err != nil {
			return nil, err
		}
		
		results = append(results, model.QueryStat{
			"route_type": routeType,
			"count":      count,
			"avg_speed":  fmt.Sprintf("%.2f", avgSpeed.Float64),
			"max_speed":  fmt.Sprintf("%.2f", maxSpeed.Float64),
		})
	}
	
	return results, rows.Err()
}

// GetSummaryStats calculates various fleet statistics.
func (r *VehicleStore) GetSummaryStats() (model.QueryStat, error) {
	stats := make(model.QueryStat)

	var totalVehicles int
	var avgSpeed, maxSpeed, minSpeed sql.NullFloat64
	err := r.db.QueryRow(`
		SELECT COUNT(*), AVG(speed), MAX(speed), MIN(speed)
		FROM vehicles
	`).Scan(&totalVehicles, &avgSpeed, &maxSpeed, &minSpeed)
	
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	stats["total_vehicles"] = totalVehicles
	stats["average_speed"] = fmt.Sprintf("%.2f mph", avgSpeed.Float64)
	stats["max_speed"] = fmt.Sprintf("%.2f mph", maxSpeed.Float64)
	stats["min_speed"] = fmt.Sprintf("%.2f mph", minSpeed.Float64)

	var inTransit, stopped, incoming int
	r.db.QueryRow(`SELECT COUNT(*) FROM vehicles WHERE current_status = 'IN_TRANSIT_TO'`).Scan(&inTransit)
	r.db.QueryRow(`SELECT COUNT(*) FROM vehicles WHERE current_status = 'STOPPED_AT'`).Scan(&stopped)
	r.db.QueryRow(`SELECT COUNT(*) FROM vehicles WHERE current_status = 'INCOMING_AT'`).Scan(&incoming)
	
	stats["in_transit"] = inTransit
	stats["stopped"] = stopped
	stats["incoming"] = incoming

	var manySeatsPct, fewSeatsPct, unknownPct sql.NullFloat64
	r.db.QueryRow(`
		SELECT 
			CAST(SUM(CASE WHEN occupancy_status = 'MANY_SEATS_AVAILABLE' THEN 1 ELSE 0 END) AS REAL) * 100.0 / COUNT(*),
			CAST(SUM(CASE WHEN occupancy_status = 'FEW_SEATS_AVAILABLE' THEN 1 ELSE 0 END) AS REAL) * 100.0 / COUNT(*),
			CAST(SUM(CASE WHEN occupancy_status = 'UNKNOWN' THEN 1 ELSE 0 END) AS REAL) * 100.0 / COUNT(*)
		FROM vehicles
	`).Scan(&manySeatsPct, &fewSeatsPct, &unknownPct)
	
	stats["occupancy_many_seats"] = fmt.Sprintf("%.1f%%", manySeatsPct.Float64)
	stats["occupancy_few_seats"] = fmt.Sprintf("%.1f%%", fewSeatsPct.Float64)
	stats["occupancy_unknown"] = fmt.Sprintf("%.1f%%", unknownPct.Float64)

	var movingVehicles, stationaryVehicles int
	r.db.QueryRow(`SELECT COUNT(*) FROM vehicles WHERE speed > 0`).Scan(&movingVehicles)
	r.db.QueryRow(`SELECT COUNT(*) FROM vehicles WHERE speed = 0`).Scan(&stationaryVehicles)
	
	stats["moving_vehicles"] = movingVehicles
	stats["stationary_vehicles"] = stationaryVehicles
	
	if totalVehicles > 0 {
		stats["percent_moving"] = fmt.Sprintf("%.1f%%", float64(movingVehicles)*100.0/float64(totalVehicles))
	} else {
		stats["percent_moving"] = "0.0%"
	}
	
	return stats, nil
}

// queryVehicles is a helper function to run a query and scan results into VehicleRecord structs.
func (r *VehicleStore) queryVehicles(query string) ([]model.VehicleRecord, error) {
	rows, err := r.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []model.VehicleRecord
	for rows.Next() {
		var r model.VehicleRecord
		var updatedAt, ingestedAt string

		err := rows.Scan(
			&r.ID, &r.Label, &r.Latitude, &r.Longitude, &r.Speed,
			&r.DirectionID, &r.CurrentStatus, &r.OccupancyStatus,
			&r.Bearing, &updatedAt, &ingestedAt,
		)
		if err != nil {
			return nil, err
		}
		
		r.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
		r.IngestedAt, _ = time.Parse(time.RFC3339, ingestedAt)
		
		records = append(records, r)
	}

	return records, rows.Err()
}

func (r *VehicleStore) Close() error {
	return r.db.Close()
}
