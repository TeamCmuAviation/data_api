import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import asyncio

from main import app, get_db

from main import get_db as main_get_db

# ---------------------------------------------------------------------
# Test DB URL - PostgreSQL asyncpg (match your environment)
# ---------------------------------------------------------------------
TEST_DATABASE_URL = "postgresql+asyncpg://postgres:toormaster@localhost/aviation"

# Will be set inside the setup fixture (so bound to the proper event loop)
TestingSessionLocal = None


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_database():
    """
    Create engine & sessionmaker inside pytest's event loop (Option A).
    Also create/drop tables and seed initial test data.
    """
    global TestingSessionLocal

    # Create engine inside pytest loop
    engine = create_async_engine(TEST_DATABASE_URL, future=True)

    TestingSessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
        class_=AsyncSession,
    )

    # Define override_get_db now that TestingSessionLocal exists
    async def override_get_db():
        async with TestingSessionLocal() as session:
            yield session

    # Register dependency override so FastAPI uses the test session
    # app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[main_get_db] = override_get_db


    # Create schema + seed data
    async with engine.begin() as conn:
        # Drop if exists (use CASCADE to be safe)
        await conn.execute(text("DROP TABLE IF EXISTS evaluation_assignments CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS human_evaluation CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS pci_scraped_accidents CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS asn_scraped_accidents CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS asrs_records CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS classification_results CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS airport_location CASCADE"))

        # Airport table
        await conn.execute(text("""
            CREATE TABLE airport_location (
                icao_code TEXT PRIMARY KEY,
                iata_code TEXT,
                name TEXT,
                city TEXT,
                country TEXT,
                lat REAL,
                lon REAL
            )
        """))
        await conn.execute(text("""
            INSERT INTO airport_location (icao_code, name)
            VALUES ('kjfk', 'John F. Kennedy International Airport')
        """))

        # Classification results
        await conn.execute(text("""
            CREATE TABLE classification_results (
                id SERIAL PRIMARY KEY,
                source_uid TEXT,
                final_category TEXT
            )
        """))
        await conn.execute(text("""
            INSERT INTO classification_results (id, source_uid, final_category)
            VALUES (1, 'asrs_1', 'Weather'),
                   (2, 'asn_1', 'Bird Strike'),
                   (3, 'asrs_2', 'Weather')
        """))

        # ASRS
        await conn.execute(text("""
            CREATE TABLE asrs_records (
                uid TEXT PRIMARY KEY,
                synopsis TEXT,
                time TEXT, -- Original string column
                sanitized_date DATE, -- New DATE column
                phase TEXT,
                aircraft_type TEXT,
                place TEXT,
                operator TEXT
            )
        """))
        await conn.execute(text("""
            INSERT INTO asrs_records
            (uid, synopsis, time, sanitized_date, phase, aircraft_type, place, operator)
            VALUES
            ('asrs_1', 'Test ASRS synopsis', '2024-01-01', '2024-01-01', 'cruise',
             'A320', 'Test City', 'Test Operator'),
            ('asrs_2', 'Another ASRS synopsis', '2024-01-15', '2024-01-15', 'climb',
             'A321', 'Test City', 'Test Operator')
        """))

        # ASN
        await conn.execute(text("""
            CREATE TABLE asn_scraped_accidents (
                uid TEXT PRIMARY KEY,
                narrative TEXT,
                date TEXT, -- Original string column
                sanitized_date DATE, -- New DATE column
                phase TEXT,
                aircraft_type TEXT,
                location TEXT,
                operator TEXT
            )
        """))
        await conn.execute(text("""
            INSERT INTO asn_scraped_accidents
            (uid, narrative, date, sanitized_date, phase, aircraft_type, location, operator)
            VALUES
            ('asn_1', 'Test ASN narrative', '2024-02-02', '2024-02-02', 'approach',
             'B737', 'Another City', 'Another Operator')
        """))

        # PCI
        await conn.execute(text("""
            CREATE TABLE pci_scraped_accidents (
                uid TEXT PRIMARY KEY,
                summary TEXT,
                date TEXT, -- Original string column
                sanitized_date DATE, -- New DATE column
                aircraft_type TEXT,
                location TEXT,
                operator TEXT
            )
        """))

        # Human evaluation
        await conn.execute(text("""
            CREATE TABLE human_evaluation (
                id SERIAL PRIMARY KEY,
                classification_result_id INTEGER,
                evaluator_id TEXT,
                human_category TEXT,
                human_confidence REAL,
                human_reasoning TEXT,
                created_at TIMESTAMP
            )
        """))

        # Assignments
        await conn.execute(text("""
            CREATE TABLE evaluation_assignments (
                assignment_id SERIAL PRIMARY KEY,
                classification_result_id INTEGER,
                evaluator_id TEXT,
                is_complete BOOLEAN,
                completed_at TIMESTAMP
            )
        """))
        await conn.execute(text("""
            INSERT INTO evaluation_assignments
            (classification_result_id, evaluator_id, is_complete)
            VALUES (101, 'test_evaluator', FALSE)
        """))

    # Yield to tests; engine & TestingSessionLocal are available globally
    yield

    # Teardown: drop tables and dispose engine
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS evaluation_assignments CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS human_evaluation CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS pci_scraped_accidents CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS asn_scraped_accidents CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS asrs_records CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS classification_results CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS airport_location CASCADE"))

    await engine.dispose()


@pytest_asyncio.fixture
async def client():
    """
    Provide an AsyncClient tied to the same asyncio loop as the test.
    ASGITransport will call the FastAPI app directly (no network).
    """
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


# ----------------- Tests -----------------

@pytest.mark.asyncio
async def test_get_airports(client):
    response = await client.get("/airports", params={"codes": ["KJFK"]})
    assert response.status_code == 200
    data = response.json()
    assert "kjfk" in data
    assert data["kjfk"]["name"] == "John F. Kennedy International Airport"


@pytest.mark.asyncio
async def test_get_airports_empty_codes(client):
    response = await client.get("/airports")
    assert response.status_code == 200
    assert response.json() == {}


@pytest.mark.asyncio
async def test_get_classification_results(client):
    response = await client.get("/classification-results")
    assert response.status_code == 200
    body = response.json()
    assert len(body) == 3
    source_uids = {row["source_uid"] for row in body}
    assert {"asrs_1", "asn_1", "asrs_2"} <= source_uids


@pytest.mark.asyncio
async def test_get_full_classification_results_bulk(client):
    response = await client.post("/full_classification_results_bulk", json=["asrs_1", "asn_1"])
    assert response.status_code == 200
    data = response.json()

    results = data["results"]
    assert "asrs_1" in results
    assert "asn_1" in results
    assert results["asrs_1"]["origin_narrative"] == "Test ASRS synopsis"
    assert results["asn_1"]["origin_narrative"] == "Test ASN narrative"

    aggregates = data["aggregates"]
    assert aggregates["total_incidents"] == 2
    assert aggregates["phase_counts"].get("cruise") == 1
    assert aggregates["phase_counts"].get("approach") == 1


@pytest.mark.asyncio
async def test_full_classification_results_bulk_empty(client):
    response = await client.post("/full_classification_results_bulk", json=[])
    assert response.status_code == 200
    assert response.json() == {"results": {}, "aggregates": {}}


@pytest.mark.asyncio
async def test_submit_human_evaluation(client):
    payload = {
        "classification_result_id": 101,
        "evaluator_id": "test_evaluator",
        "human_category": "Test Category",
        "human_confidence": 0.99,
        "human_reasoning": "This is a test."
    }

    response = await client.post("/human_evaluation/submit", json=payload)
    assert response.status_code == 200
    assert response.json()["status"] == "success"

    async with TestingSessionLocal() as session:
        res_eval = await session.execute(
            text("""
                SELECT classification_result_id, evaluator_id,
                       human_category, human_confidence, human_reasoning
                FROM human_evaluation
                WHERE classification_result_id = :c_id
                  AND evaluator_id = :e_id
            """),
            {"c_id": 101, "e_id": "test_evaluator"},
        )
        row = res_eval.mappings().first()
        assert row is not None
        assert row["human_category"] == "Test Category"
        assert pytest.approx(row["human_confidence"], rel=1e-6) == 0.99
        assert row["human_reasoning"] == "This is a test."

        res_assign = await session.execute(
            text("""
                SELECT is_complete, completed_at
                FROM evaluation_assignments
                WHERE classification_result_id = :c_id
                  AND evaluator_id = :e_id
            """),
            {"c_id": 101, "e_id": "test_evaluator"},
        )
        assign_row = res_assign.mappings().first()
        assert assign_row is not None
        assert assign_row["is_complete"] in (1, True)
        assert assign_row["completed_at"] is not None


@pytest.mark.asyncio
async def test_get_aggregates_over_time(client):
    # monthly
    response_month = await client.get("/aggregates/over-time", params={"period": "month"})
    assert response_month.status_code == 200
    data_month = response_month.json()

    # Convert to a dictionary for easier lookup, as order isn't guaranteed
    month_map = {d["period_start"]: d["incident_count"] for d in data_month}
    assert month_map.get("2024-01") == 2  # asrs_1 and asrs_2
    assert month_map.get("2024-02") == 1  # asn_1

    # yearly
    response_year = await client.get("/aggregates/over-time", params={"period": "year"})
    assert response_year.status_code == 200
    data_year = response_year.json()
    assert len(data_year) >= 1
    # EXTRACT may return a Decimal — cast to int for assertion
    assert int(data_year[0]["period_start"]) == 2024
    # Total incidents for 2024 should be 3 (two ASRS + one ASN)
    total_incidents = sum(int(r["incident_count"]) for r in data_year)
    assert total_incidents == 3


@pytest.mark.asyncio
async def test_get_top_n_aggregates(client):
    response = await client.get("/aggregates/top-n", params={"category": "operator", "n": 5})
    assert response.status_code == 200
    data = response.json()
    found = {row["category_value"] for row in data}
    assert {"Test Operator", "Another Operator"}.issubset(found)


@pytest.mark.asyncio
async def test_get_top_n_aggregates_by_final_category(client):
    """
    Tests the top-n aggregation for the 'final_category' dimension,
    which requires joining with the classification_results table.
    """
    response = await client.get("/aggregates/top-n", params={"category": "final_category", "n": 5})
    assert response.status_code == 200
    data = response.json()

    # Expected: 'Weather' (count 2), 'Bird Strike' (count 1)
    assert len(data) == 2
    assert data[0]["category_value"] == "Weather"
    assert data[0]["incident_count"] == 2
    assert data[1]["category_value"] == "Bird Strike"
    assert data[1]["incident_count"] == 1


@pytest.mark.asyncio
async def test_get_top_n_aggregates_by_final_category_with_date_filter(client):
    """
    Tests the top-n aggregation for 'final_category' with a date range filter.
    """
    # Filter for January 2024 - should only include the two 'Weather' incidents
    response_jan = await client.get(
        "/aggregates/top-n",
        params={"category": "final_category", "n": 5, "start_period": "2024-01", "end_period": "2024-01"}
    )
    assert response_jan.status_code == 200
    data_jan = response_jan.json()
    assert len(data_jan) == 1
    assert data_jan[0]["category_value"] == "Weather"
    assert data_jan[0]["incident_count"] == 2

    # Filter for February 2024 - should only include the 'Bird Strike' incident
    response_feb = await client.get(
        "/aggregates/top-n",
        params={"category": "final_category", "n": 5, "start_period": "2024-02", "end_period": "2024-02"}
    )
    assert response_feb.status_code == 200
    data_feb = response_feb.json()
    assert len(data_feb) == 1
    assert data_feb[0]["category_value"] == "Bird Strike"
    assert data_feb[0]["incident_count"] == 1

    # Filter for a period with no incidents
    response_empty = await client.get(
        "/aggregates/top-n",
        params={"category": "final_category", "n": 5, "start_period": "2025-01", "end_period": "2025-01"}
    )
    assert response_empty.status_code == 200
    assert response_empty.json() == []


@pytest.mark.asyncio
async def test_get_incident_locations(client):
    async with TestingSessionLocal() as session:
        await session.execute(text("""
            INSERT INTO asrs_records (uid, time, sanitized_date, place)
            VALUES ('asrs_with_loc', '2024-03-15', '2024-03-15', 'kjfk')
        """))
        await session.commit()

    response = await client.get("/incidents/locations")
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    # find the inserted item
    found = [d for d in data if d["uid"] == "asrs_with_loc"]
    assert len(found) == 1
    assert found[0]["location_name"] == "John F. Kennedy International Airport"

    # Test date filter that excludes the incident
    response_filtered = await client.get("/incidents/locations?start_date=2025-01-01")
    assert response_filtered.status_code == 200
    assert response_filtered.json() == []


@pytest.mark.asyncio
async def test_get_statistics(client):
    response = await client.get("/aggregates/statistics")
    assert response.status_code == 200
    data = response.json()
    # Based on seed data: asrs_1, asn_1, and asrs_with_loc from another test
    assert data["total_incidents"] >= 2
