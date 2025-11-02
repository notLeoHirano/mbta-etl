package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/notLeoHirano/mbta-etl/etl"
	api "github.com/notLeoHirano/mbta-etl/mbta"
	"github.com/notLeoHirano/mbta-etl/transformer"
	"github.com/notLeoHirano/mbta-etl/vehiclestore"
)

// initDependencies sets up all components of the pipeline.
func initDependencies(apiURL, dbPath string) (*etl.Pipeline, vehiclestore.Repository, error) {
	// VehicleStore (L and Q)
	repo, err := vehiclestore.NewVehicleStore(dbPath) // Correctly uses NewVehicleStore
	if err != nil {
		return nil, nil, fmt.Errorf("dependency setup failed (vehiclestore): %w", err)
	}

	// API Client (E)
	apiClient := api.NewMBTAClient(apiURL)

	// Transformer (T)
	xformer := transformer.NewVehicleTransformer()

	// Pipeline (Orchestration)
	pipeline := etl.NewPipeline(apiClient, xformer, repo)

	return pipeline, repo, nil
}

func main() {
	// CLI flags
	runETL := flag.Bool("run", false, "Run the ETL pipeline once")
	query := flag.String("query", "", "Query to run (top10, routes, stats)")
	dbPath := flag.String("db", "mbta_vehicles.db", "Database path")
	apiURL := flag.String("api", "https://api-v3.mbta.com/vehicles", "MBTA API URL")

	flag.Parse()

	// Setup dependencies
	pipeline, repo, err := initDependencies(*apiURL, *dbPath)
	if err != nil {
		log.Fatalf("Initialization error: %v", err)
	}
	defer repo.Close() // Ensure the database connection is closed

	if *runETL {
		if err := pipeline.Run(); err != nil {
			log.Fatalf("ETL pipeline failed: %v", err)
		}
		fmt.Println("ETL pipeline completed successfully!")
		return
	}

	// Handle Query execution
	switch *query {
	case "top10":
		vehicles, err := repo.GetTop10FastestVehicles()
		if err != nil {
			log.Fatalf("Query failed: %v", err)
		}
		
		fmt.Println("\nTop 10 Fastest Vehicles")
		for i, v := range vehicles {
			fmt.Printf("%d. Vehicle %s (Label: %s) - Speed: %.2f mph, Status: %s (Last seen: %v)\n",
				i+1, v.ID, v.Label, v.Speed, v.CurrentStatus, v.UpdatedAt.Format("2006-01-02 15:04:05"))
		}

	case "routes":
		routes, err := repo.GetRouteBreakdown()
		if err != nil {
			log.Fatalf("Query failed: %v", err)
		}
		
		fmt.Println("\nMBTA ROUTE BREAKDOWN")
		fmt.Println()
		fmt.Printf("%-20s %10s %15s %15s\n", "Route Type", "Count", "Avg Speed", "Max Speed")
		fmt.Println("─────────────────────────────────────────────────────────────")
		
		for _, route := range routes {
			fmt.Printf("%-20s %10v %12s mph %12s mph\n",
				route["route_type"], route["count"], route["avg_speed"], route["max_speed"])
		}
		fmt.Println()

	case "stats":
		stats, err := repo.GetSummaryStats()
		if err != nil {
			log.Fatalf("Query failed: %v", err)
		}
		
		fmt.Println("\nMBTA VEHICLE SUMMARY STATISTICS")
		
		fmt.Println("\nFLEET OVERVIEW")
		fmt.Printf("   Total Vehicles: %v\n", stats["total_vehicles"])
		fmt.Printf("   Moving: %v (%v)\n", stats["moving_vehicles"], stats["percent_moving"])
		fmt.Printf("   Stationary: %v\n", stats["stationary_vehicles"])
		
		fmt.Println("\nSPEED METRICS")
		fmt.Printf("   Average Speed: %v\n", stats["average_speed"])
		fmt.Printf("   Max Speed: %v\n", stats["max_speed"])
		
		fmt.Println("\nVEHICLE STATUS")
		fmt.Printf("   In Transit: %v\n", stats["in_transit"])
		fmt.Printf("   Stopped: %v\n", stats["stopped"])
		fmt.Printf("   Incoming: %v\n", stats["incoming"])
		
		fmt.Println("\nOCCUPANCY LEVELS")
		fmt.Printf("   Many Seats Available: %v\n", stats["occupancy_many_seats"])
		fmt.Printf("   Few Seats Available: %v\n", stats["occupancy_few_seats"])
		fmt.Printf("   Unknown: %v\n", stats["occupancy_unknown"])
		
		fmt.Println("\nDIRECTION")
		fmt.Printf("   Outbound (Direction 0): %v\n", stats["outbound_vehicles"])
		fmt.Printf("   Inbound (Direction 1): %v\n", stats["inbound_vehicles"])
		fmt.Println()

	default:
		// Default help message
		fmt.Println("MBTA Vehicle Data ETL & Analysis Tool")
		fmt.Println("\nUsage:")
		fmt.Println("  Run ETL:        go run . -run")
		fmt.Println("  Query top 10:   go run . -query top10")
		fmt.Println("  Query stats:    go run . -query stats")
		fmt.Println("  Query routes:   go run . -query routes")
		fmt.Println("\nOptional Flags:")
		fmt.Println("  -db [path]: Specify database file path (default: mbta_vehicles.db)")
		os.Exit(1)
	}
}
