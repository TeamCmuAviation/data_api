## FastAPI Data API (Dockerized)

This project exposes a simple FastAPI service that queries a PostgreSQL database for aviation-related records based on a `uid`. The table is inferred from the prefix before the first `_` in the `uid` (e.g. `asn_`, `asrs_`, `pci_`).

### Endpoints

---

### API Specification

#### **GET `/airports`**
- **Description**: Retrieves detailed information for a list of airports based on their ICAO codes.
- **Query Parameters**:
  - `codes` (List[str]): A list of ICAO airport codes to fetch. Example: `?codes=KJFK&codes=EGLL`
- **Success Response (200 OK)**:
  - **Content-Type**: `application/json`
  - **Body**: A dictionary where keys are the requested ICAO codes and values are objects containing airport details.
  ```json
  {
    "KJFK": {
      "icao_code": "KJFK",
      "iata_code": "JFK",
      "name": "John F. Kennedy International Airport",
      "city": "New York",
      "country": "United States",
      "lat": 40.639801,
      "lon": -73.7789
    }
  }
  ```

#### **GET `/classification-results`**
- **Description**: Fetches all records from the `classification_results` table.
- **Query Parameters**: None.
- **Success Response (200 OK)**:
  - **Content-Type**: `application/json`
  - **Body**: A list of classification result objects.
  ```json
  [
    {
      "id": 1,
      "source_uid": "asrs_12345",
      "bert_results": "...",
      "llm1_category": "...",
      "final_category": "...",
      "processed_at": "2023-10-27T10:00:00Z"
    }
  ]
  ```

#### **POST `/full_classification_results_bulk`**
- **Description**: Retrieves full, joined classification results for a list of UIDs. This endpoint combines data from `classification_results` with the original source report (e.g., `asrs_records`). It also provides aggregated statistics for the returned data.
- **Request Body**:
  - **Content-Type**: `application/json`
  - **Body**: A list of `source_uid` strings.
  ```json
  ["asrs_12345", "asn_67890"]
  ```
- **Success Response (200 OK)**:
  - **Content-Type**: `application/json`
  - **Body**: An object containing `results` (a dictionary of the full records keyed by `source_uid`) and `aggregates` (summary statistics).
  ```json
  {
    "results": {
      "asrs_12345": {
        "id": 1,
        "source_uid": "asrs_12345",
        "final_category": "...",
        "origin_uid": "asrs_12345",
        "origin_date": "...",
        "origin_phase": "...",
        "origin_aircraft_type": "...",
        "origin_location": "...",
        "origin_operator": "...",
        "origin_narrative": "..."
      }
    },
    "aggregates": {
      "total_incidents": 120,
      "unique_operators": 45,
      "unique_aircraft_types": 88,
      "phase_counts": { "Cruise": 50, "Landing": 30 },
      "operator_counts": { "Operator A": 25, "Operator B": 15 }
    }
  }
  ```

#### **POST `/human_evaluation/submit`**
- **Description**: Submits a human-in-the-loop evaluation for a specific classification result. This action inserts a record into the `human_evaluation` table and marks the corresponding task in `evaluation_assignments` as complete.
- **Request Body**:
  - **Content-Type**: `application/json`
  - **Body**: A JSON object containing the evaluation details.
  ```json
  {
    "classification_result_id": 101,
    "evaluator_id": "john.doe",
    "human_category": "Human Verified Category",
    "human_confidence": 0.95,
    "human_reasoning": "The narrative clearly indicates pilot error during the landing phase."
  }
  ```
- **Success Response (200 OK)**:
  - **Content-Type**: `application/json`
  - **Body**: A confirmation message.
  ```json
  {
    "status": "success",
    "message": "Evaluation submitted"
  }
  ```

### Database Configuration

The database connection is configured in `database.py` via `DATABASE_URL`. Ensure that:

- The PostgreSQL instance is reachable from inside the Docker container.
- The credentials and host in `DATABASE_URL` are correct for your environment.

If you prefer, you can change `DATABASE_URL` to read from an environment variable and then pass it at `docker run` time.

### Building the Docker Image

From the project root (where `Dockerfile` is located), run:

```bash
docker build -t data-api .
```

### Running the Container

Run the container, exposing port `8000`:

```bash
docker run --rm -p 58510:58510 data-api
```

If you modify `DATABASE_URL` to come from an environment variable (e.g. `DATABASE_URL`), you can pass it like this:

```bash
docker run --rm -p 58510:58510 -e DATABASE_URL="postgresql://user:password@host:5432/dbname" data-api
```

### Accessing the API

Once the container is running, you can access:

- **Endpoint**: `http://localhost:8000/record/{uid}`
  - Example:
    - `http://localhost:8000/record/asn_example_uid`
    - `http://localhost:8000/record/asrs_example_uid`
    - `http://localhost:8000/record/pci_example_uid`

### Interactive API Docs

FastAPI automatically provides interactive docs:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
