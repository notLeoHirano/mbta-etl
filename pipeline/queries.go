package pipeline

import (
	"fmt"
)

// Query functions
func (p *ETLPipeline) GetTop10FastestVehicles() ([]VehicleRecord, error) {
	query := `
		SELECT id, label, latitude, longitude, speed, direction_id, current_status, occupancy_status, bearing, updated_at, ingested_at
		FROM vehicles
		ORDER BY speed DESC
		LIMIT 10
	`
	return p.queryVehicles(query)
}

func (p *ETLPipeline) GetRouteBreakdown() ([]map[string]interface{}, error) {
	// Extract route prefix from vehicle ID (e.g., "R-" for Red, "G-" for Green, "O-" for Orange)
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
	
	rows, err := p.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var routeType string
		var count int
		var avgSpeed, maxSpeed float64
		
		err := rows.Scan(&routeType, &count, &avgSpeed, &maxSpeed)
		if err != nil {
			return nil, err
		}
		
		results = append(results, map[string]interface{}{
			"route_type": routeType,
			"count":      count,
			"avg_speed":  fmt.Sprintf("%.2f", avgSpeed),
			"max_speed":  fmt.Sprintf("%.2f", maxSpeed),
		})
	}
	
	return results, rows.Err()
}

func (p *ETLPipeline) GetSummaryStats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// Basic stats
	var totalVehicles int
	var avgSpeed, maxSpeed, minSpeed float64
	err := p.db.QueryRow(`
		SELECT COUNT(*), AVG(speed), MAX(speed), MIN(speed)
		FROM vehicles
	`).Scan(&totalVehicles, &avgSpeed, &maxSpeed, &minSpeed)
	
	if err != nil {
		return nil, err
	}

	stats["total_vehicles"] = totalVehicles
	stats["average_speed"] = fmt.Sprintf("%.2f mph", avgSpeed)
	stats["max_speed"] = fmt.Sprintf("%.2f mph", maxSpeed)
	stats["min_speed"] = fmt.Sprintf("%.2f mph", minSpeed)

	// Vehicles by status
	var inTransit, stopped, incoming int
	p.db.QueryRow(`SELECT COUNT(*) FROM vehicles WHERE current_status = 'IN_TRANSIT_TO'`).Scan(&inTransit)
	p.db.QueryRow(`SELECT COUNT(*) FROM vehicles WHERE current_status = 'STOPPED_AT'`).Scan(&stopped)
	p.db.QueryRow(`SELECT COUNT(*) FROM vehicles WHERE current_status = 'INCOMING_AT'`).Scan(&incoming)
	
	stats["in_transit"] = inTransit
	stats["stopped"] = stopped
	stats["incoming"] = incoming

	// Occupancy distribution
	var manySeatsPct, fewSeatsPct, unknownPct float64
	p.db.QueryRow(`
		SELECT 
			CAST(SUM(CASE WHEN occupancy_status = 'MANY_SEATS_AVAILABLE' THEN 1 ELSE 0 END) AS FLOAT) * 100.0 / COUNT(*),
			CAST(SUM(CASE WHEN occupancy_status = 'FEW_SEATS_AVAILABLE' THEN 1 ELSE 0 END) AS FLOAT) * 100.0 / COUNT(*),
			CAST(SUM(CASE WHEN occupancy_status = 'UNKNOWN' THEN 1 ELSE 0 END) AS FLOAT) * 100.0 / COUNT(*)
		FROM vehicles
	`).Scan(&manySeatsPct, &fewSeatsPct, &unknownPct)
	
	stats["occupancy_many_seats"] = fmt.Sprintf("%.1f%%", manySeatsPct)
	stats["occupancy_few_seats"] = fmt.Sprintf("%.1f%%", fewSeatsPct)
	stats["occupancy_unknown"] = fmt.Sprintf("%.1f%%", unknownPct)

	// Direction distribution
	var direction0, direction1 int
	p.db.QueryRow(`SELECT COUNT(*) FROM vehicles WHERE direction_id = 0`).Scan(&direction0)
	p.db.QueryRow(`SELECT COUNT(*) FROM vehicles WHERE direction_id = 1`).Scan(&direction1)
	
	stats["outbound_vehicles"] = direction0
	stats["inbound_vehicles"] = direction1

	// Active vs stationary vehicles
	var movingVehicles, stationaryVehicles int
	p.db.QueryRow(`SELECT COUNT(*) FROM vehicles WHERE speed > 0`).Scan(&movingVehicles)
	p.db.QueryRow(`SELECT COUNT(*) FROM vehicles WHERE speed = 0`).Scan(&stationaryVehicles)
	
	stats["moving_vehicles"] = movingVehicles
	stats["stationary_vehicles"] = stationaryVehicles
	
	if totalVehicles > 0 {
		stats["percent_moving"] = fmt.Sprintf("%.1f%%", float64(movingVehicles)*100.0/float64(totalVehicles))
	}

	// Speed percentiles for moving vehicles
	var p50, p90, p95 float64
	p.db.QueryRow(`
		SELECT speed FROM vehicles WHERE speed > 0 
		ORDER BY speed LIMIT 1 OFFSET (SELECT COUNT(*) FROM vehicles WHERE speed > 0) / 2
	`).Scan(&p50)
	p.db.QueryRow(`
		SELECT speed FROM vehicles WHERE speed > 0 
		ORDER BY speed LIMIT 1 OFFSET (SELECT COUNT(*) FROM vehicles WHERE speed > 0) * 9 / 10
	`).Scan(&p90)
	p.db.QueryRow(`
		SELECT speed FROM vehicles WHERE speed > 0 
		ORDER BY speed LIMIT 1 OFFSET (SELECT COUNT(*) FROM vehicles WHERE speed > 0) * 95 / 100
	`).Scan(&p95)
	
	if movingVehicles > 0 {
		stats["median_speed"] = fmt.Sprintf("%.2f mph", p50)
		stats["speed_90th_percentile"] = fmt.Sprintf("%.2f mph", p90)
		stats["speed_95th_percentile"] = fmt.Sprintf("%.2f mph", p95)
	}

	return stats, nil
}

func (p *ETLPipeline) queryVehicles(query string) ([]VehicleRecord, error) {
	rows, err := p.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []VehicleRecord
	for rows.Next() {
		var r VehicleRecord
		err := rows.Scan(
			&r.ID, &r.Label, &r.Latitude, &r.Longitude, &r.Speed,
			&r.DirectionID, &r.CurrentStatus, &r.OccupancyStatus,
			&r.Bearing, &r.UpdatedAt, &r.IngestedAt,
		)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}

	return records, rows.Err()
}

func (p *ETLPipeline) GetVehiclesByBearing(target float64, delta float64) ([]VehicleRecord, error) {
    minBearing := target - delta
    maxBearing := target + delta

    query := `
        SELECT id, label, latitude, longitude, speed, direction_id, current_status, occupancy_status, bearing, updated_at, ingested_at
        FROM vehicles
        WHERE bearing BETWEEN ? AND ?
    `

    rows, err := p.db.Query(query, minBearing, maxBearing)
    if err != nil {
        return nil, fmt.Errorf("failed to query vehicles by bearing: %w", err)
    }
    defer rows.Close()

    var results []VehicleRecord
    for rows.Next() {
        var v VehicleRecord
        if err := rows.Scan(
            &v.ID, &v.Label, &v.Latitude, &v.Longitude, &v.Speed,
            &v.DirectionID, &v.CurrentStatus, &v.OccupancyStatus,
            &v.Bearing, &v.UpdatedAt, &v.IngestedAt,
        ); err != nil {
            return nil, err
        }
        results = append(results, v)
    }

    return results, nil
}

func (p *ETLPipeline) GetBearingSummary() (map[string]int, error) {
    // Cardinal directions with approximate ranges
    directions := map[string][2]float64{
        "North":     {337.5, 22.5},  // Wraps around 0
        "Northeast": {22.5, 67.5},
        "East":      {67.5, 112.5},
        "Southeast": {112.5, 157.5},
        "South":     {157.5, 202.5},
        "Southwest": {202.5, 247.5},
        "West":      {247.5, 292.5},
        "Northwest": {292.5, 337.5},
    }

    summary := make(map[string]int)

    // Initialize counts
    for dir := range directions {
        summary[dir] = 0
    }

    rows, err := p.db.Query("SELECT bearing FROM vehicles")
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    for rows.Next() {
        var bearing int
        if err := rows.Scan(&bearing); err != nil {
            return nil, err
        }

        // Determine which cardinal direction it belongs to
        found := false
        for dir, r := range directions {
            min, max := r[0], r[1]
            if dir == "North" && (float64(bearing) >= min || float64(bearing) < max) {
                summary[dir]++
                found = true
                break
            } else if dir != "North" && float64(bearing) >= min && float64(bearing) < max {
                summary[dir]++
                found = true
                break
            }
        }
        if !found {
            summary["North"]++ // fallback if bearing is exactly 360
        }
    }

    return summary, nil
}

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
