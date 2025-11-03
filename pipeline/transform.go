package pipeline

import (
	"log"
	"time"
)

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