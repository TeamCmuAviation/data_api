# Data API

This FastAPI application provides a comprehensive set of endpoints to query and analyze aviation incident data. It allows fetching raw incident reports, classification results, and various aggregated statistics suitable for data visualization.

## Features

*   **Raw Data Access**: Fetch detailed incident reports from multiple sources.
*   **Aggregated Statistics**: Get aggregated data for various dimensions:
    *   Counts over time (time-series).
    *   Top-N rankings for categories like operators, aircraft, and locations.
    *   Hierarchical data for sunburst/treemap visualizations.
    *   Heatmap data for 2D analysis.
*   **Geographic Data**: Retrieve incidents with coordinates for map visualizations.
*   **Human-in-the-loop**: Endpoints to support human evaluation and classification workflows.

## Getting Started

### Prerequisites

*   Python 3.8+
*   PostgreSQL

### Installation

1.  Clone the repository and navigate into the directory.
2.  Install the required Python packages. It is recommended to use a virtual environment.
    ```bash
    pip install -r requirements.txt
    ```
3.  Ensure your PostgreSQL database is running and accessible. The application is configured via environment variables or a `.env` file to connect to the database. The tests in `test_api.py` use the connection string `postgresql+asyncpg://postgres:toormaster@localhost/aviation`.

### Running the API

To run the API locally with live reloading, use `uvicorn`. The server will be available at `http://0.0.0.0:58510` by default.

```bash
uvicorn main:app --host 0.0.0.0 --port 58510 --reload
```

## API Endpoints

This section provides a detailed specification for each API endpoint.

### General

#### `GET /airports`

**Description**: Get details for a list of airports by ICAO code.

**Query Parameters**:
-   `codes` (`list[str]`, optional): A list of ICAO airport codes.

**Example Request**:
`GET /airports?codes=KLAX&codes=KJFK`

**Example Response**:
A dictionary where keys are ICAO codes and values are airport details.
```json
{
  "KLAX": {
    "icao_code": "KLAX",
    "iata_code": "LAX",
    "name": "Los Angeles International Airport",
    "city": "Los Angeles",
    "country": "USA",
    "lat": 33.942501,
    "lon": -118.407997
  },
  "KJFK": {
    "icao_code": "KJFK",
    "iata_code": "JFK",
    "name": "John F. Kennedy International Airport",
    "city": "New York",
    "country": "USA",
    "lat": 40.639801,
    "lon": -73.7789
  }
}
```

### Classification & Evaluation

#### `GET /classification-results`

**Description**: Get a paginated list of classification results. Can be filtered by evaluator.

**Query Parameters**:
-   `skip` (`int`, optional, default: 0): Number of records to skip for pagination.
-   `limit` (`int`, optional, default: 100): Maximum number of records to return.
-   `evaluator_id` (`str`, optional): Filter results assigned to a specific evaluator.

**Example Request**:
`GET /classification-results?skip=0&limit=10&evaluator_id=EVAL01`

**Example Response**:
A list of classification result objects.
```json
[
  {
    "id": 1,
    "source_uid": "uid-123",
    "model_version": "v1.0",
    "predicted_category": "Weather",
    "predicted_confidence": 0.88,
    "final_category": "Weather",
    "is_complete": false,
    "evaluator_id": "EVAL01"
  }
]
```

#### `POST /full_classification_results_bulk`

**Description**: Get full details for a bulk list of incident UIDs, including original report data and summary aggregates.

**Request Body**:
A JSON array of incident UIDs.
```json
[
    "uid-1",
    "uid-2"
]
```

**Example Response**:
An object containing `results` (a dictionary of full incident details keyed by UID) and `aggregates` (summary statistics for the requested incidents).
```json
{
  "results": {
    "uid-1": {
      "id": 1,
      "source_uid": "uid-1",
      "model_version": "v1.0",
      "predicted_category": "Bird Strike",
      "predicted_confidence": 0.99,
      "final_category": "Bird Strike",
      "origin_uid": "uid-1",
      "origin_date": "2023-05-10",
      "origin_phase": "Take-off",
      "origin_aircraft_type": "Boeing 737",
      "origin_location": "KLAX",
      "origin_operator": "American Airlines",
      "origin_narrative": "The aircraft struck a bird during takeoff..."
    }
  },
  "aggregates": {
    "total_incidents": 2,
    "unique_operators": 1,
    "unique_aircraft_types": 2,
    "phase_counts": {
      "Take-off": 1,
      "Landing": 1
    },
    "operator_counts": {
      "American Airlines": 2
    }
  }
}
```

#### `POST /human_evaluation/submit`

**Description**: Inserts a human evaluation record for a classification result and marks the corresponding evaluation assignment as complete.

**Request Body**:
```json
{
  "classification_result_id": 123,
  "evaluator_id": "EVAL01",
  "human_category": "Weather",
  "human_confidence": 0.95,
  "human_reasoning": "The narrative clearly indicates severe weather conditions."
}
```

**Example Response**:
```json
{
  "status": "success",
  "message": "Evaluation submitted"
}
```

### Aggregation Endpoints

These endpoints provide aggregated data and support a common set of filters.

**Common Filters**:
-   `operators` (`list[str]`, optional): Filter by one or more operators.
-   `phases` (`list[str]`, optional): Filter by one or more flight phases.
-   `aircraft_types` (`list[str]`, optional): Filter by one or more aircraft types.
-   `locations` (`list[str]`, optional): Filter by one or more locations (ICAO codes). *(Not available on all endpoints)*
-   `start_period` (`str`, optional): Start period in `YYYY-MM` format (e.g., "2022-01").
-   `end_period` (`str`, optional): End period in `YYYY-MM` format (e.g., "2022-12").

---

#### `GET /aggregates/over-time`

**Description**: Provides aggregated incident counts over time (by year or month). This is useful for creating time-series visualizations.

**Query Parameters**:
-   `period` (`str`, optional, default: "month", enum: `["year", "month"]`): The time period to group by.
-   *Plus Common Filters (excluding `locations`)*

**Example Request**:
`GET /aggregates/over-time?period=year&operators=American+Airlines`

**Example Response**:
A list of objects, each containing a time period and the corresponding incident count.
```json
[
  {
    "period_start": "2022",
    "incident_count": 150
  },
  {
    "period_start": "2023",
    "incident_count": 175
  }
]
```

#### `GET /aggregates/top-n`

**Description**: Provides a list of the top N most frequent items for a given category, such as 'operator', 'aircraft_type', 'phase', 'location', or 'final_category'.

**Query Parameters**:
-   `category` (`str`, required, enum: `["operator", "aircraft_type", "phase", "final_category", "location"]`): The category to aggregate.
-   `n` (`int`, optional, default: 10): The number of top results to return.
-   *Plus Common Filters (all available)*

**Example Request**:
`GET /aggregates/top-n?category=aircraft_type&n=5`

**Example Response**:
A list of objects, each containing a category value and its incident count.
```json
[
  {
    "category_value": "Boeing 737",
    "incident_count": 500
  },
  {
    "category_value": "Airbus A320",
    "incident_count": 450
  }
]
```

**Example Request (by classification category)**:
`GET /aggregates/top-n?category=final_category&n=3`

**Example Response (by classification category)**:
A list of objects, each containing a category value and its incident count.
```json
[
  {
    "category_value": "Bird Strike",
    "incident_count": 830
  },
  {
    "category_value": "Weather",
    "incident_count": 620
  },
  {
    "category_value": "Engine Failure",
    "incident_count": 410
  }
]
```

#### `GET /aggregates/classification-over-time`

**Description**: Provides aggregated time-series data for classification results, supporting filtering by classification, phase, location, and aircraft type.

**Query Parameters**:
-   `period` (`str`, optional, default: "month", enum: `["year", "month"]`): The time period to group by.
-   `final_categories` (`list[str]`, optional): Filter by one or more final classification categories.
-   *Plus Common Filters (excluding `operators`)*

**Example Request**:
`GET /aggregates/classification-over-time?period=month&final_categories=Bird+Strike`

**Example Response**:
A list of objects, each containing a time period and the corresponding incident count.
```json
[
  {
    "period_start": "2023-01",
    "incident_count": 12
  },
  {
    "period_start": "2023-02",
    "incident_count": 9
  }
]
```

#### `GET /aggregates/hierarchy`

**Description**: Provides data grouped by operator, aircraft_type, and phase, suitable for hierarchical visualizations like sunburst or treemap charts.

**Query Parameters**:
-   *Common Filters (all available)*

**Example Request**:
`GET /aggregates/hierarchy?operators=Delta+Air+Lines`

**Example Response**:
A list of objects, each representing a unique combination of operator, aircraft type, and phase with its incident count.
```json
[
  {
    "operator": "Delta Air Lines",
    "aircraft_type": "Boeing 737",
    "phase": "Cruise",
    "incident_count": 5
  },
  {
    "operator": "Delta Air Lines",
    "aircraft_type": "Airbus A321",
    "phase": "Landing",
    "incident_count": 3
  }
]
```

#### `GET /aggregates/heatmap`

**Description**: Provides aggregated data suitable for a heatmap visualization, showing the relationship between two categorical dimensions.

**Query Parameters**:
-   `dimension1` (`str`, required, enum: `["operator", "aircraft_type", "phase"]`): The first dimension for the heatmap.
-   `dimension2` (`str`, required, enum: `["operator", "aircraft_type", "phase"]`): The second dimension for the heatmap.
-   *Plus Common Filters (all available)*

**Example Request**:
`GET /aggregates/heatmap?dimension1=phase&dimension2=aircraft_type`

**Example Response**:
A list of objects, each representing a cell in the heatmap with its incident count.
```json
[
  {
    "dim1_value": "Take-off",
    "dim2_value": "Boeing 737",
    "incident_count": 150
  },
  {
    "dim1_value": "Landing",
    "dim2_value": "Airbus A320",
    "incident_count": 120
  }
]
```

#### `GET /aggregates/statistics`

**Description**: Provides high-level summary statistics, including the total number of incidents.

**Query Parameters**:
-   *Common Filters (all available)*

**Example Request**:
`GET /aggregates/statistics?start_period=2022-01&end_period=2022-12`

**Example Response**:
An object containing the total number of incidents matching the filters.
```json
{
  "total_incidents": 1234
}
```

### Geographic Endpoints

#### `GET /incidents/locations`

**Description**: Provides a list of incidents with their geographic coordinates, suitable for map visualizations.

**Query Parameters**:
-   *Plus Common Filters (excluding `locations`)*

**Example Request**:
`GET /incidents/locations?start_period=2023-01&end_period=2023-03`

**Example Response**:
A list of incident objects with geographic information.
```json
[
  {
    "uid": "asn-123",
    "summary": "Aircraft experienced engine failure during climb...",
    "origin_date": "2023-01-15",
    "operator": "United Airlines",
    "lat": 40.6413,
    "lon": -73.7781,
    "location_name": "John F Kennedy International Airport"
  }
]
```