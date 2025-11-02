package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/notLeoHirano/mbta-etl/model"
)

// Client defines the interface for fetching external data.
type Client interface {
	FetchVehicles() (*model.VehicleResponse, error)
}

// MBTAClient implements the Client interface for the MBTA V3 API.
type MBTAClient struct {
	apiURL string
	client *http.Client
}

func NewMBTAClient(url string) *MBTAClient {
	return &MBTAClient{
		apiURL: url,
		client: &http.Client{Timeout: 10 * time.Second}, 
	}
}

// FetchVehicles fetches and decodes the vehicle data.
func (c *MBTAClient) FetchVehicles() (*model.VehicleResponse, error) {
	resp, err := c.client.Get(c.apiURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Read body for better error context
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
	}

	var vehicleResp model.VehicleResponse
	if err := json.NewDecoder(resp.Body).Decode(&vehicleResp); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return &vehicleResp, nil
}
