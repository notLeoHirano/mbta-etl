# MBTA Vehicles ETL Pipeline

A simple ETL pipeline that ingests real-time vehicle data from the MBTA API and stores it in SQLite for querying and analysis.

## Features

- **Extract**: Fetches live vehicle data from the MBTA V3 API
- **Transform**: Cleans and normalizes text fields
- **Load**: Persists data to SQLite with UPSERT support (handles duplicates)
- **Query Interface**: CLI commands for data exploration
- **Comprehensive Testing**: 10+ unit tests covering success and error cases
- **Clean Architecture**: Clear separation of concerns (ETL layers)

### Data Schema

The pipeline normalizes MBTA vehicle data into the following schema:

| Column           | Type      | Description                    |
| ---------------- | --------- | ------------------------------ |
| id               | TEXT      | Unique vehicle identifier (PK) |
| label            | TEXT      | Vehicle label/number           |
| latitude         | REAL      | Current latitude               |
| longitude        | REAL      | Current longitude              |
| speed            | REAL      | Current speed (mph, 0 if null) |
| direction_id     | INTEGER   | Direction (0 or 1)             |
| current_status   | TEXT      | Status (e.g., IN_TRANSIT_TO)   |
| occupancy_status | TEXT      | Occupancy level                |
| bearing          | INTEGER   | Compass bearing (0 if null)    |
| updated_at       | TIMESTAMP | Last update from MBTA          |
| ingested_at      | TIMESTAMP | When record was ingested       |

## Installation

0. **Ensure you have a recent version of Go installed**

1. **Clone the repository:**

   ```bash
   git clone `https://github.com/notLeoHirano/mbta-etl.git`
   cd mbta-etl
   ```

2. **Install dependencies:**

   ```bash
   go mod init mbta-etl
   go get modernc.org/sqlite
   go mod tidy
   ```

## Usage

### Run the ETL Pipeline

Fetch current vehicle data from MBTA and load into the database:

```bash
go run main.go -run
```

Output:

```bash
2025/11/02 18:27:38 Extracting data from MBTA API...
2025/11/02 18:27:39 Extracted 373 vehicles
2025/11/02 18:27:39 Transforming data...
2025/11/02 18:27:39 Transformed 373 records
2025/11/02 18:27:39 Loading data to database...
2025/11/02 18:27:39 Successfully loaded 373 records

ETL pipeline completed successfully!

Usage:
  Run ETL:             go run main.go -run
  Query top 10:        go run main.go -query top10
  Query stats:         go run main.go -query stats
  Query routes:        go run main.go -query routes
  Query by bearing:    go run main.go -query bearing -bearing 90 -delta 15
  Get bearing summary: go run main.go -query bearing_summary
```

### Query Top 10 Fastest Vehicles

```bash
go run main.go -query top10
```

Output:

```
Top 10 Fastest Vehicles
1. Vehicle 1838 (Label: 1838) - Speed: 33.50 mph, Status: IN_TRANSIT_TO
2. Vehicle 1713 (Label: 1713) - Speed: 28.20 mph, Status: IN_TRANSIT_TO
3. Vehicle 1846 (Label: 1846) - Speed: 28.20 mph, Status: IN_TRANSIT_TO
...
```

### Query Summary Statistics

```bash
go run main.go -query stats
```

Output:

```bash
MBTA VEHICLE SUMMARY STATISTICS

FLEET OVERVIEW
   Total Vehicles: 522
   Moving: 55 (10.5%)
   Stationary: 467

SPEED METRICS
   Average Speed: 1.01 mph
...
```

### Query by vehicle direction

```bash
go run main.go -query bearing -bearing 90 -delta 15
```

Output:

```bash
Vehicles with Bearing 90.0 ± 15.0 degrees

Vehicle ID Label      Bearing    Speed
─────────────────────────────────────────────
y3214      3214       90         0.00
y2039      2039       101        0.00
y1874      1874       81         0.00
y1793      1793       80         0.00
...
```

### Or an overview of direction

```bash
go run main.go -query bearing_summary
```

Output:

```bash
Vehicle Bearing Summary

Direction            Count
───────────────────────────
Northeast               72
East                    48
South                   40
West                    44
North                  164
Southeast               53
Southwest               52
Northwest               49
```

### Custom Database Path

```bash
go run main.go -run -db custom_path.db
```

### Custom API URL

```bash
go run main.go -run -api "https://api-v3.mbta.com/vehicles?filter[route]=Red"
```

## Running Tests

Execute all unit tests:

```bash
go test -v
```

The test suite tests:

- **Extract - Successful API call**: Validates data fetching
- **Extract - API error status**: Tests error handling for API failures
- **Extract - Invalid JSON**: Tests malformed response handling
- **Transform - Nullable fields**: Validates default value handling
- **Transform - Invalid records**: Tests filtering of bad data
- **Transform - Status normalization**: Tests status field cleaning
- **Load - Success**: Validates data persistence
- **Load - Duplicates (UPSERT)**: Tests update behavior
- **Query - Top 10 fastest**: Tests sorting and limiting
- **Query - Summary stats**: Tests aggregation functions

## API Reference

### MBTA V3 API

- **Endpoint**: `https://api-v3.mbta.com/vehicles`
- **Documentation**: `https://api-v3.mbta.com/docs/swagger/index.html`
- **Rate Limit**: 1000 requests per minute (unauthenticated)
- **No API Key Required** for basic vehicle data

### Filtering Options

Filter by route:

```bash
go run main.go -run -api "https://api-v3.mbta.com/vehicles?filter[route]=Red"
```

Filter by direction:

```bash
go run main.go -run -api "https://api-v3.mbta.com/vehicles?filter[direction_id]=0"
```
