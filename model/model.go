package model

import "time"

// MBTA API Response structures
type VehicleResponse struct {
	Data []Vehicle `json:"data"`
}

type Vehicle struct {
	ID         string     `json:"id"`
	Type       string     `json:"type"`
	Attributes Attributes `json:"attributes"`
}

type Attributes struct {
	UpdatedAt           string  `json:"updated_at"`
	Speed               *float64 `json:"speed"`
	RevenueStatus       string  `json:"revenue_status"`
	OccupancyStatus     string  `json:"occupancy_status"`
	Longitude           float64 `json:"longitude"`
	Latitude            float64 `json:"latitude"`
	Label               string  `json:"label"`
	DirectionID         int     `json:"direction_id"`
	CurrentStopSequence *int     `json:"current_stop_sequence"`
	CurrentStatus       string  `json:"current_status"`
	Bearing             *int     `json:"bearing"`
}

// VehicleRecord is the normalized struct to be stored in the db
type VehicleRecord struct {
	ID              string
	Label           string
	Latitude        float64
	Longitude       float64
	Speed           float64
	DirectionID     int
	CurrentStatus   string
	OccupancyStatus string
	Bearing         int
	UpdatedAt       time.Time
	IngestedAt      time.Time
}

// QueryStat is a generic map used for returning summary statistics.
type QueryStat map[string]interface{}
