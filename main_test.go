package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/notLeoHirano/mbta-etl/pipeline"

	. "github.com/notLeoHirano/mbta-etl/model"
)

// Test Extract - Successful API call
func TestExtractSuccess(t *testing.T) {
	mockResponse := `{
		"data": [
			{
				"id": "test-vehicle-1",
				"type": "vehicle",
				"attributes": {
					"updated_at": "2024-01-15T10:30:00-05:00",
					"speed": 25.5,
					"revenue_status": "REVENUE",
					"occupancy_status": "MANY_SEATS_AVAILABLE",
					"longitude": -71.0589,
					"latitude": 42.3601,
					"label": "1234",
					"direction_id": 0,
					"current_stop_sequence": 5,
					"current_status": "IN_TRANSIT_TO",
					"bearing": 180
				}
			}
		]
	}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	p, err := pipeline.NewETLPipeline(server.URL, ":memory:")
	if err != nil {
		t.Fatalf("Failed to create p: %v", err)
	}
	defer p.Close()

	resp, err := p.Extract()
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}

	if len(resp.Data) != 1 {
		t.Errorf("Expected 1 vehicle, got %d", len(resp.Data))
	}

	if resp.Data[0].ID != "test-vehicle-1" {
		t.Errorf("Expected vehicle ID 'test-vehicle-1', got '%s'", resp.Data[0].ID)
	}

	if resp.Data[0].Attributes.Label != "1234" {
		t.Errorf("Expected label '1234', got '%s'", resp.Data[0].Attributes.Label)
	}
}

// Test Extract - API returns error status
func TestExtractAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	p, err := pipeline.NewETLPipeline(server.URL, ":memory:")
	if err != nil {
		t.Fatalf("Failed to create p: %v", err)
	}
	defer p.Close()

	_, err = p.Extract()
	if err == nil {
		t.Error("Expected error for API error status, got nil")
	}
}

// Test Extract - Invalid JSON response
func TestExtractInvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	p, err := pipeline.NewETLPipeline(server.URL, ":memory:")
	if err != nil {
		t.Fatalf("Failed to create p: %v", err)
	}
	defer p.Close()

	_, err = p.Extract()
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

// Test Transform - Handles nullable fields
func TestTransformNullableFields(t *testing.T) {
	vehicles := []Vehicle{
		{
			ID:   "test-1",
			Type: "vehicle",
			Attributes: Attributes{
				UpdatedAt:       "2024-01-15T10:30:00-05:00",
				Speed:           nil, // Nullable field
				Bearing:         nil, // Nullable field
				RevenueStatus:   "REVENUE",
				OccupancyStatus: "MANY_SEATS_AVAILABLE",
				Longitude:       -71.0589,
				Latitude:        42.3601,
				Label:           "1234",
				DirectionID:     0,
				CurrentStatus:   "IN_TRANSIT_TO",
			},
		},
	}

	p, err := pipeline.NewETLPipeline("http://test", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create p: %v", err)
	}
	defer p.Close()

	records, err := p.Transform(vehicles)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(records))
	}

	// Check defaults for nullable fields
	if records[0].Speed != 0.0 {
		t.Errorf("Expected speed 0.0 for nil, got %.2f", records[0].Speed)
	}

	if records[0].Bearing != 0 {
		t.Errorf("Expected bearing 0 for nil, got %d", records[0].Bearing)
	}
}

// Test Transform - Filters invalid records
func TestTransformFiltersInvalidRecords(t *testing.T) {
	vehicles := []Vehicle{
		{
			ID:   "", // Invalid - empty ID
			Type: "vehicle",
			Attributes: Attributes{
				UpdatedAt: "2024-01-15T10:30:00-05:00",
				Label:     "1234",
				Latitude:  42.3601,
				Longitude: -71.0589,
			},
		},
		{
			ID:   "test-2",
			Type: "vehicle",
			Attributes: Attributes{
				UpdatedAt: "2024-01-15T10:30:00-05:00",
				Label:     "", // Invalid - empty label
				Latitude:  42.3601,
				Longitude: -71.0589,
			},
		},
		{
			ID:   "test-3",
			Type: "vehicle",
			Attributes: Attributes{
				UpdatedAt: "2024-01-15T10:30:00-05:00",
				Label:     "5678",
				Latitude:  42.3601,
				Longitude: -71.0589,
			},
		},
	}

	p, err := pipeline.NewETLPipeline("http://test", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create p: %v", err)
	}
	defer p.Close()

	records, err := p.Transform(vehicles)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	// Should only keep the valid record
	if len(records) != 1 {
		t.Errorf("Expected 1 valid record, got %d", len(records))
	}

	if records[0].ID != "test-3" {
		t.Errorf("Expected valid record with ID 'test-3', got '%s'", records[0].ID)
	}
}

// Test Transform - Normalizes status fields
func TestTransformNormalizesStatus(t *testing.T) {
	vehicles := []Vehicle{
		{
			ID:   "test-1",
			Type: "vehicle",
			Attributes: Attributes{
				UpdatedAt:       "2024-01-15T10:30:00-05:00",
				Label:           "1234",
				Latitude:        42.3601,
				Longitude:       -71.0589,
				CurrentStatus:   "",                 // Empty status
				OccupancyStatus: "MANY_SEATS_AVAILABLE", // Normal status
			},
		},
	}

	p, err := pipeline.NewETLPipeline("http://test", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create p: %v", err)
	}
	defer p.Close()

	records, err := p.Transform(vehicles)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	if records[0].CurrentStatus != "UNKNOWN" {
		t.Errorf("Expected 'UNKNOWN' for empty status, got '%s'", records[0].CurrentStatus)
	}

	if records[0].OccupancyStatus != "MANY_SEATS_AVAILABLE" {
		t.Errorf("Expected 'MANY_SEATS_AVAILABLE', got '%s'", records[0].OccupancyStatus)
	}
}

// Test Load - Successfully stores records
func TestLoadSuccess(t *testing.T) {
	// Create temporary database
	tmpfile, err := os.CreateTemp("", "test*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	p, err := pipeline.NewETLPipeline("http://test", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create p: %v", err)
	}
	defer p.Close()

	records := []VehicleRecord{
		{
			ID:              "test-1",
			Label:           "1234",
			Latitude:        42.3601,
			Longitude:       -71.0589,
			Speed:           25.5,
			DirectionID:     0,
			CurrentStatus:   "IN_TRANSIT_TO",
			OccupancyStatus: "MANY_SEATS_AVAILABLE",
			Bearing:         180,
			UpdatedAt:       time.Now(),
			IngestedAt:      time.Now(),
		},
	}

	err = p.Load(records)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Verify data was stored
  count, err := p.CountVehicles()

	if err != nil {
		t.Fatalf("Failed to query database: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 record in database, got %d", count)
	}
}

// Test Load - Handles duplicate records (UPSERT)
func TestLoadHandlesDuplicates(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	p, err := pipeline.NewETLPipeline("http://test", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create p: %v", err)
	}
	defer p.Close()

	record := VehicleRecord{
		ID:              "test-1",
		Label:           "1234",
		Latitude:        42.3601,
		Longitude:       -71.0589,
		Speed:           25.5,
		DirectionID:     0,
		CurrentStatus:   "IN_TRANSIT_TO",
		OccupancyStatus: "MANY_SEATS_AVAILABLE",
		Bearing:         180,
		UpdatedAt:       time.Now(),
		IngestedAt:      time.Now(),
	}

	// Load first time
	err = p.Load([]VehicleRecord{record})
	if err != nil {
		t.Fatalf("First load failed: %v", err)
	}

	// Load again with updated speed
	record.Speed = 30.0
	err = p.Load([]VehicleRecord{record})
	if err != nil {
		t.Fatalf("Second load failed: %v", err)
	}

	// Should still have only 1 record
	count, err := p.CountVehicles()
	if err != nil {
		t.Fatalf("Failed to query database: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 record (upserted), got %d", count)
	}

	// Verify speed was updated
	speed, err := p.GetVehicleSpeed(record.ID)
	if err != nil {
		t.Fatalf("Failed to query speed: %v", err)
	}

	if speed != 30.0 {
		t.Errorf("Expected updated speed 30.0, got %.2f", speed)
	}
}

// Test Query - Top 10 fastest vehicles
func TestGetTop10FastestVehicles(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	p, err := pipeline.NewETLPipeline("http://test", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create p: %v", err)
	}
	defer p.Close()

	// Insert test data with varying speeds
	records := []VehicleRecord{}
	for i := 0; i < 15; i++ {
		records = append(records, VehicleRecord{
			ID:              string(rune('A' + i)),
			Label:           string(rune('1' + i)),
			Latitude:        42.3601,
			Longitude:       -71.0589,
			Speed:           float64(i * 5), // 0, 5, 10, 15, ... 70
			DirectionID:     0,
			CurrentStatus:   "IN_TRANSIT_TO",
			OccupancyStatus: "MANY_SEATS_AVAILABLE",
			Bearing:         180,
			UpdatedAt:       time.Now(),
			IngestedAt:      time.Now(),
		})
	}

	err = p.Load(records)
	if err != nil {
		t.Fatalf("Failed to load test data: %v", err)
	}

	// Query top 10
	top10, err := p.GetTop10FastestVehicles()
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(top10) != 10 {
		t.Errorf("Expected 10 vehicles, got %d", len(top10))
	}

	// Verify they are sorted by speed (descending)
	for i := 0; i < len(top10)-1; i++ {
		if top10[i].Speed < top10[i+1].Speed {
			t.Errorf("Results not sorted: vehicle %d (speed %.2f) < vehicle %d (speed %.2f)",
				i, top10[i].Speed, i+1, top10[i+1].Speed)
		}
	}

	// Fastest should be 70 mph
	if top10[0].Speed != 70.0 {
		t.Errorf("Expected fastest vehicle at 70 mph, got %.2f", top10[0].Speed)
	}
}

// Test Query - Summary statistics
func TestGetSummaryStats(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test*.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	tmpfile.Close()

	p, err := pipeline.NewETLPipeline("http://test", tmpfile.Name())
	if err != nil {
		t.Fatalf("Failed to create p: %v", err)
	}
	defer p.Close()

	// Insert test data
	records := []VehicleRecord{
		{
			ID: "1", Label: "A", Latitude: 42.3601, Longitude: -71.0589,
			Speed: 10.0, DirectionID: 0, CurrentStatus: "IN_TRANSIT_TO",
			OccupancyStatus: "MANY_SEATS_AVAILABLE", Bearing: 180,
			UpdatedAt: time.Now(), IngestedAt: time.Now(),
		},
		{
			ID: "2", Label: "B", Latitude: 42.3601, Longitude: -71.0589,
			Speed: 20.0, DirectionID: 0, CurrentStatus: "IN_TRANSIT_TO",
			OccupancyStatus: "MANY_SEATS_AVAILABLE", Bearing: 180,
			UpdatedAt: time.Now(), IngestedAt: time.Now(),
		},
		{
			ID: "3", Label: "C", Latitude: 42.3601, Longitude: -71.0589,
			Speed: 30.0, DirectionID: 0, CurrentStatus: "IN_TRANSIT_TO",
			OccupancyStatus: "MANY_SEATS_AVAILABLE", Bearing: 180,
			UpdatedAt: time.Now(), IngestedAt: time.Now(),
		},
	}

	err = p.Load(records)
	if err != nil {
		t.Fatalf("Failed to load test data: %v", err)
	}

	stats, err := p.GetSummaryStats()
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	avgSpeedStr, ok := stats["average_speed"].(string)
	if !ok {
			t.Fatalf("average_speed is not a string")
	}
	avgSpeedStr = strings.TrimSuffix(avgSpeedStr, " mph")
	avgSpeed, err := strconv.ParseFloat(avgSpeedStr, 64)
	if err != nil {
			t.Fatalf("Failed to parse average_speed: %v", err)
	}
	if avgSpeed != 20.0 {
			t.Errorf("Expected average speed 20.0, got %.2f", avgSpeed)
	}

	maxSpeedStr, ok := stats["max_speed"].(string)
	if !ok {
			t.Fatalf("max_speed is not a string")
	}
	maxSpeedStr = strings.TrimSuffix(maxSpeedStr, " mph")
	maxSpeed, err := strconv.ParseFloat(maxSpeedStr, 64)
	if err != nil {
			t.Fatalf("Failed to parse max_speed: %v", err)
	}
	if maxSpeed != 30.0 {
			t.Errorf("Expected max speed 30.0, got %.2f", maxSpeed)
	}
}