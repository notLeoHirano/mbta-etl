package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/notLeoHirano/mbta-etl/pipeline"
	_ "modernc.org/sqlite"
)


func main() {
	// CLI flags
	runETL := flag.Bool("run", false, "Run the ETL pipeline")
	query := flag.String("query", "", "Query to run (top10, stats)")
	dbPath := flag.String("db", "mbta_vehicles.db", "Database path")
	apiURL := flag.String("api", "https://api-v3.mbta.com/vehicles", "MBTA API URL")

	flag.Parse()

	pipeline, err := pipeline.NewETLPipeline(*apiURL, *dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize pipeline: %v", err)
	}
	defer pipeline.Close()

	if *runETL {
		if err := pipeline.Run(); err != nil {
			log.Fatalf("ETL pipeline failed: %v", err)
		}
		fmt.Println("ETL pipeline completed successfully!")
		return
	}

	switch *query {
	case "top10":
		vehicles, err := pipeline.GetTop10FastestVehicles()
		if err != nil {
			log.Fatalf("Query failed: %v", err)
		}
		
		fmt.Println("\n=== Top 10 Fastest Vehicles ===")
		for i, v := range vehicles {
			fmt.Printf("%d. Vehicle %s (Label: %s) - Speed: %.2f mph, Status: %s\n",
				i+1, v.ID, v.Label, v.Speed, v.CurrentStatus)
		}

	case "routes":
		routes, err := pipeline.GetRouteBreakdown()
		if err != nil {
			log.Fatalf("Query failed: %v", err)
		}
		
		fmt.Println("\n╔═══════════════════════════════════════════════════════╗")
		fmt.Println("║              MBTA ROUTE BREAKDOWN                     ║")
		fmt.Println("╚═══════════════════════════════════════════════════════╝")
		fmt.Println()
		fmt.Printf("%-20s %10s %15s %15s\n", "Route Type", "Count", "Avg Speed", "Max Speed")
		fmt.Println("─────────────────────────────────────────────────────────────")
		
		for _, route := range routes {
			fmt.Printf("%-20s %10v %12s mph %12s mph\n",
				route["route_type"], route["count"], route["avg_speed"], route["max_speed"])
		}
		fmt.Println()

	case "stats":
		stats, err := pipeline.GetSummaryStats()
		if err != nil {
			log.Fatalf("Query failed: %v", err)
		}
		
		fmt.Println("\n╔═══════════════════════════════════════════════════════╗")
		fmt.Println("║          MBTA VEHICLE SUMMARY STATISTICS              ║")
		fmt.Println("╚═══════════════════════════════════════════════════════╝")
		
		fmt.Println("\nFLEET OVERVIEW")
		fmt.Printf("   Total Vehicles: %v\n", stats["total_vehicles"])
		fmt.Printf("   Moving: %v (%v)\n", stats["moving_vehicles"], stats["percent_moving"])
		fmt.Printf("   Stationary: %v\n", stats["stationary_vehicles"])
		
		fmt.Println("\nSPEED METRICS")
		fmt.Printf("   Average Speed: %v\n", stats["average_speed"])
		fmt.Printf("   Median Speed: %v\n", stats["median_speed"])
		fmt.Printf("   Max Speed: %v\n", stats["max_speed"])
		fmt.Printf("   90th Percentile: %v\n", stats["speed_90th_percentile"])
		fmt.Printf("   95th Percentile: %v\n", stats["speed_95th_percentile"])
		
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
		fmt.Println("Usage:")
		fmt.Println("  Run ETL:        go run main.go -run")
		fmt.Println("  Query top 10:   go run main.go -query top10")
		fmt.Println("  Query stats:    go run main.go -query stats")
		fmt.Println("  Query routes:   go run main.go -query routes")
		os.Exit(1)
	}
}