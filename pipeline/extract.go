package pipeline

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

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
