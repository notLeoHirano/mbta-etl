package transformer

import (
	"log"
	"time"

	"github.com/notLeoHirano/mbta-etl/model"
)

// Transformer defines the interface for data transformation.
type Transformer interface {
	TransformVehicles(vehicles []model.Vehicle) ([]model.VehicleRecord, error)
}

// VehicleTransformer implements the Transformer interface.
type VehicleTransformer struct{}

func NewVehicleTransformer() *VehicleTransformer {
	return &VehicleTransformer{}
}

// TransformVehicles cleans and normalizes the raw vehicle data into database records.
func (t *VehicleTransformer) TransformVehicles(vehicles []model.Vehicle) ([]model.VehicleRecord, error) {
	records := make([]model.VehicleRecord, 0, len(vehicles))
	now := time.Now().UTC()

	for _, v := range vehicles {
		// 1. Basic validation check
		if v.ID == "" || v.Attributes.Label == "" {
			log.Printf("Warning: Skipping vehicle with empty ID or Label.")
			continue
		}

		// 2. Parse timestamp
		updatedAt, err := time.Parse(time.RFC3339, v.Attributes.UpdatedAt)
		if err != nil {
			log.Printf("Warning: Failed to parse timestamp for vehicle %s (%s). Using current time.", v.ID, v.Attributes.UpdatedAt)
			updatedAt = now
		}

		// 3. Handle nullable fields and provide defaults (Coalescing)
		speed := 0.0
		if v.Attributes.Speed != nil {
			speed = *v.Attributes.Speed
		}

		bearing := 0
		if v.Attributes.Bearing != nil {
			bearing = *v.Attributes.Bearing
		}
		
		// Optional: Ensure Speed is non-negative
		if speed < 0 {
		    speed = 0.0
		}

		// 4. Normalize status fields
		currentStatus := normalizeStatus(v.Attributes.CurrentStatus)
		occupancyStatus := normalizeStatus(v.Attributes.OccupancyStatus)

		record := model.VehicleRecord{
			ID:              v.ID,
			Label:           v.Attributes.Label,
			Latitude:        v.Attributes.Latitude,
			Longitude:       v.Attributes.Longitude,
			Speed:           speed,
			DirectionID:     v.Attributes.DirectionID,
			CurrentStatus:   currentStatus,
			OccupancyStatus: occupancyStatus,
			Bearing:         bearing,
			UpdatedAt:       updatedAt.UTC(), // Always store time in UTC
			IngestedAt:      now,
		}

		records = append(records, record)
	}

	return records, nil
}

// normalizeStatus ensures status fields are consistent, using UNKNOWN as default.
func normalizeStatus(status string) string {
	if status == "" {
		return "UNKNOWN"
	}
	return status
}
