package pipeline

import "fmt"

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
