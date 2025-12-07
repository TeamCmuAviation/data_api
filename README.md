## FastAPI Data API (Dockerized)

This project exposes a simple FastAPI service that queries a PostgreSQL database for aviation-related records based on a `uid`. The table is inferred from the prefix before the first `_` in the `uid` (e.g. `asn_`, `asrs_`, `pci_`).

## API Endpoints

---

### 1. Get Airport Information

Retrieves details for a list of airports based on their ICAO codes.

*   **Endpoint:** `GET /airports`
*   **Query Parameters:**
    *   `codes` (list[str]): A list of ICAO airport codes (e.g., `KJFK`, `EGLL`).
*   **Success Response (200):**
    *   Returns a dictionary where keys are the ICAO codes and values are objects containing airport details.

    **Example Request:**
    ```
    GET /airports?codes=KJFK&codes=EGLL
    ```

    **Example Response:**
    ```json
    {
      "KJFK": {
        "icao_code": "KJFK",
        "iata_code": "JFK",
        "name": "John F Kennedy International Airport",
        "city": "New York",
        "country": "USA",
        "lat": 40.639801,
        "lon": -73.7789
      },
      "EGLL": {
        "icao_code": "EGLL",
        "iata_code": "LHR",
        "name": "London Heathrow Airport",
        "city": "London",
        "country": "GB",
        "lat": 51.4706,
        "lon": -0.461941
      }
    }
    ```

---

### 2. Get Classification Results

Fetches a paginated list of classification results. Can be filtered by the evaluator assigned to the result.

*   **Endpoint:** `GET /classification-results`
*   **Query Parameters:**
    *   `skip` (int, optional, default: 0): Number of records to skip for pagination.
    *   `limit` (int, optional, default: 100): Maximum number of records to return.
    *   `evaluator_id` (str, optional): Filter results by the ID of the evaluator.
*   **Success Response (200):**
    *   Returns a list of classification result objects.

---

### 3. Get Full Classification Results in Bulk

Retrieves comprehensive details for multiple classification results, including original source data and aggregate statistics.

*   **Endpoint:** `POST /full_classification_results_bulk`
*   **Request Body:**
    *   A JSON list of source UIDs.
    ```json
    [ "uid1", "uid2" ]
    ```
*   **Success Response (200):**
    *   Returns an object containing:
        *   `results`: A dictionary of the full result data, keyed by `source_uid`.
        *   `aggregates`: An object with aggregate statistics like total incidents, unique operators, etc.

---

### 4. Submit Human Evaluation

Submits a human-provided evaluation for a specific classification result and marks the corresponding evaluation assignment as complete.

*   **Endpoint:** `POST /human_evaluation/submit`
*   **Request Body:**
    *   A JSON object with the following fields:
        *   `classification_result_id` (int): The ID of the classification result being evaluated.
        *   `evaluator_id` (str): The ID of the person performing the evaluation.
        *   `human_category` (str): The category assigned by the human evaluator.
        *   `human_confidence` (float): The confidence level of the human evaluator.
        *   `human_reasoning` (str): The reasoning behind the human evaluation.
*   **Success Response (200):**
    ```json
    {
      "status": "success",
      "message": "Evaluation submitted"
    }
    ```
*   **Error Response (200):**
    *   If the assignment is not found or already completed.
    ```json
    {
      "status": "error",
      "message": "Assignment not found or already complete."
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
