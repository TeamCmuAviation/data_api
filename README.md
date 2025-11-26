## FastAPI Data API (Dockerized)

This project exposes a simple FastAPI service that queries a PostgreSQL database for aviation-related records based on a `uid`. The table is inferred from the prefix before the first `_` in the `uid` (e.g. `asn_`, `asrs_`, `pci_`).

### Endpoints

- **GET `/record/{uid}`**  
  - Infers the table from the `uid` prefix:
    - `asn_...` → `public.asn_scraped_accidents`
    - `asrs_...` → `public.asrs_records`
    - `pci_...` → `public.pci_scraped_accidents`
  - Returns JSON with:
    - `uid`, `date`, `phase`, `aircraft_type`, `location`, `operator`, `narrative`

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
docker run --rm -p 8000:8000 data-api
```

If you modify `DATABASE_URL` to come from an environment variable (e.g. `DATABASE_URL`), you can pass it like this:

```bash
docker run --rm -p 8000:8000 -e DATABASE_URL="postgresql://user:password@host:5432/dbname" data-api
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


