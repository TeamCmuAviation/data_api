# Aviation Safety Analysis API

This project provides a high-performance FastAPI backend designed to serve data for an aviation safety analysis dashboard. It offers a suite of API endpoints for retrieving raw incident data, fetching pre-aggregated analytics for visualizations, and managing human evaluation workflows.

The API is built to be efficient and scalable, pushing complex filtering and aggregation logic to the database layer to ensure the frontend remains fast and responsive.

## System Architecture

The system is composed of:

*   **FastAPI Application (`main.py`):** A modern, high-performance Python web framework for building APIs.
*   **PostgreSQL Database:** A robust, open-source relational database to store all aviation incident data, classification results, and user evaluations.
*   **SQLAlchemy Core:** Used for asynchronous database interaction, allowing for non-blocking I/O and high concurrency.
*   **Docker & Docker Compose:** For containerizing the application and its database, ensuring a consistent and reproducible development and deployment environment.

## Features

*   **Bulk Data Retrieval:** Efficiently fetch detailed information for thousands of incident records in a single request.
*   **Dynamic Aggregation Endpoints:**
    *   Time-series data for incident trends (`/aggregates/over-time`).
    *   Top-N rankings for categories like operators, aircraft, and flight phases (`/aggregates/top-n`).
    *   Geospatial data for map visualizations (`/incidents/locations`).
    *   Two-dimensional data for correlation heatmaps (`/aggregates/heatmap`).
    *   Hierarchical data for sunburst or treemap charts (`/aggregates/hierarchy`).
*   **Interactive Filtering:** Most aggregation endpoints support filtering by date range, operator, aircraft type, and more.
*   **Human Evaluation Workflow:** Endpoints to support a "human-in-the-loop" review process for classification results.
*   **Containerized Deployment:** Ready to be deployed with Docker.

---

## Getting Started

### Prerequisites

*   Docker and Docker Compose
*   Python 3.10+ (for local development outside of Docker)

### Deployment with Docker (Recommended)

This is the simplest and most reliable way to run the application and its database.

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd data_api
    ```

2.  **Configure the Database:**
    The Docker Compose setup uses the environment variables defined in `docker-compose.yml`. The application's `DATABASE_URL` is automatically configured to connect to the Dockerized PostgreSQL instance.

3.  **Build and Run the Containers:**
    From the root of the project directory, run:
    ```bash
    docker-compose up --build
    ```
    This command will:
    *   Build the Docker image for the FastAPI application.
    *   Start a PostgreSQL container.
    *   Start the FastAPI application container.

4.  **Access the API:**
    The API will be available at `http://localhost:8000`. You can access the interactive OpenAPI documentation at `http://localhost:8000/docs`.

### Local Development (Without Docker)

1.  **Set up a Python virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure the Database:**
    Ensure you have a running PostgreSQL instance. Modify the `DATABASE_URL` in `database.py` to point to your database.

4.  **Run the application:**
    ```bash
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
    ```

---

## Running Tests

The test suite uses an in-memory SQLite database to ensure tests are fast and isolated.

To run the tests (either locally or inside the Docker container):
```bash
pytest
```

---

## API Specification

Below is a summary of the available endpoints. For complete details and interactive testing, please refer to the auto-generated docs at `/docs` when the application is running.

### `GET /airports`
*   **Description:** Retrieves location and metadata for a list of airports.
*   **Query Parameters:** `codes` (List[str]): A list of ICAO codes to look up.
*   **Example:** `GET /airports?codes=KJFK&codes=EGLL`

### `GET /classification-results`
*   **Description:** Fetches a paginated list of all classification results.
*   **Query Parameters:** `skip` (int), `limit` (int), `evaluator_id` (str, optional).

### `POST /full_classification_results_bulk`
*   **Description:** Retrieves the full, joined incident records for a given list of UIDs. This is the primary endpoint for populating a data table.
*   **Request Body:** A JSON array of string UIDs. `["asrs_1", "asn_1"]`
*   **Response:** A JSON object containing `results` (a dictionary of incident objects) and `aggregates` (summary statistics).

### `GET /aggregates/over-time`
*   **Description:** Provides time-series data of incident counts.
*   **Query Parameters:**
    *   `period` ('year' or 'month'): The time bucket for aggregation.
    *   `start_period` (str, optional): Start period in `YYYY-MM` format (e.g., "2023-01").
    *   `end_period` (str, optional): End period in `YYYY-MM` format (e.g., "2023-12").
    *   `operators` (List[str], optional): List of operators to filter by.
    *   `phases` (List[str], optional): List of flight phases to filter by.
    *   `aircraft_types` (List[str], optional): List of aircraft types to filter by.
*   **Example:** `GET /aggregates/over-time?period=month&operators=American+Airlines&start_period=2023-01`

### `GET /aggregates/top-n`
*   **Description:** Gets the top N most frequent items for a given category.
*   **Query Parameters:**
    *   `category` ('operator', 'aircraft_type', 'phase', 'location'): The category to rank.
    *   `n` (int): The number of results to return.
    *   `start_period` (str, optional): Start period in `YYYY-MM` format.
    *   `end_period` (str, optional): End period in `YYYY-MM` format.
    *   `operators` (List[str], optional): List of operators to filter by.
    *   `phases` (List[str], optional): List of flight phases to filter by.
    *   `aircraft_types` (List[str], optional): List of aircraft types to filter by.
    *   `locations` (List[str], optional): List of location ICAO codes to filter by.
*   **Example:** `GET /aggregates/top-n?category=aircraft_type&n=5&phases=approach`

### `GET /incidents/locations`
*   **Description:** Provides geolocated incidents for map visualizations.
*   **Query Parameters:**
    *   `start_period` (str, optional): Start period in `YYYY-MM` format.
    *   `end_period` (str, optional): End period in `YYYY-MM` format.
    *   `operators` (List[str], optional): List of operators to filter by.
    *   `phases` (List[str], optional): List of flight phases to filter by.
    *   `aircraft_types` (List[str], optional): List of aircraft types to filter by.
*   **Example:** `GET /incidents/locations?start_period=2023-01&end_period=2023-03`

### `GET /aggregates/heatmap`
*   **Description:** Provides 2D aggregated data for generating a correlation heatmap.
*   **Query Parameters:**
    *   `dimension1`, `dimension2` ('operator', 'aircraft_type', 'phase'): The two categories to cross-tabulate.
    *   `start_period` (str, optional): Start period in `YYYY-MM` format.
    *   `end_period` (str, optional): End period in `YYYY-MM` format.
    *   `operators` (List[str], optional): List of operators to filter by.
    *   `phases` (List[str], optional): List of flight phases to filter by.
    *   `aircraft_types` (List[str], optional): List of aircraft types to filter by.
    *   `locations` (List[str], optional): List of location ICAO codes to filter by.
*   **Example:** `GET /aggregates/heatmap?dimension1=phase&dimension2=aircraft_type&operators=Delta+Air+Lines`

### `GET /aggregates/hierarchy`
*   **Description:** Provides data grouped by operator, aircraft type, and phase for hierarchical charts (e.g., sunburst).
*   **Query Parameters:**
    *   `start_period` (str, optional): Start period in `YYYY-MM` format.
    *   `end_period` (str, optional): End period in `YYYY-MM` format.
    *   `operators` (List[str], optional): List of operators to filter by.
    *   `phases` (List[str], optional): List of flight phases to filter by.
    *   `aircraft_types` (List[str], optional): List of aircraft types to filter by.
    *   `locations` (List[str], optional): List of location ICAO codes to filter by.

### `GET /aggregates/statistics`
*   **Description:** Provides high-level summary statistics, like total incident count.
*   **Query Parameters:**
    *   `start_period` (str, optional): Start period in `YYYY-MM` format.
    *   `end_period` (str, optional): End period in `YYYY-MM` format.
    *   `operators` (List[str], optional): List of operators to filter by.
    *   `phases` (List[str], optional): List of flight phases to filter by.
    *   `aircraft_types` (List[str], optional): List of aircraft types to filter by.
    *   `locations` (List[str], optional): List of location ICAO codes to filter by.

### `POST /human_evaluation/submit`
*   **Description:** Submits a human evaluation for a specific classification result.
*   **Request Body:** A JSON object with evaluation details.

```

### 2. New `Dockerfile`

This file contains the instructions to build a Docker image for your FastAPI application.

```diff