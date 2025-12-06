import pytest
import pytest_asyncio

from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from main import app, get_db


TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

test_engine = create_async_engine(TEST_DATABASE_URL, future=True)
TestingSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=test_engine,
    class_=AsyncSession,
)


async def override_get_db():
    async with TestingSessionLocal() as session:
        yield session


app.dependency_overrides[get_db] = override_get_db


@pytest_asyncio.fixture(autouse=True)
async def setup_test_database():
    async with test_engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS evaluation_assignments"))
        await conn.execute(text("DROP TABLE IF EXISTS human_evaluation"))
        await conn.execute(text("DROP TABLE IF EXISTS pci_scraped_accidents"))
        await conn.execute(text("DROP TABLE IF EXISTS asn_scraped_accidents"))
        await conn.execute(text("DROP TABLE IF EXISTS asrs_records"))
        await conn.execute(text("DROP TABLE IF EXISTS classification_results"))
        await conn.execute(text("DROP TABLE IF EXISTS airport_location"))

        await conn.execute(
            text(
                """
                CREATE TABLE airport_location (
                    icao_code TEXT PRIMARY KEY,
                    iata_code TEXT,
                    name TEXT,
                    city TEXT,
                    country TEXT,
                    lat REAL,
                    lon REAL
                )
                """
            )
        )
        await conn.execute(
            text(
                "INSERT INTO airport_location (icao_code, name) "
                "VALUES ('kjfk', 'John F. Kennedy International Airport')"
            )
        )

        await conn.execute(
            text(
                """
                CREATE TABLE classification_results (
                    id INTEGER PRIMARY KEY,
                    source_uid TEXT
                )
                """
            )
        )
        await conn.execute(
            text(
                """
                INSERT INTO classification_results (id, source_uid)
                VALUES (1, 'asrs_1'), (2, 'asn_1')
                """
            )
        )

        await conn.execute(
            text(
                """
                CREATE TABLE asrs_records (
                    uid TEXT PRIMARY KEY,
                    synopsis TEXT,
                    time TEXT,
                    phase TEXT,
                    aircraft_type TEXT,
                    place TEXT,
                    operator TEXT
                )
                """
            )
        )
        await conn.execute(
            text(
                """
                INSERT INTO asrs_records
                    (uid, synopsis, time, phase, aircraft_type, place, operator)
                VALUES
                    ('asrs_1', 'Test ASRS synopsis', '2024-01-01', 'cruise',
                     'A320', 'Test City', 'Test Operator')
                """
            )
        )

        await conn.execute(
            text(
                """
                CREATE TABLE asn_scraped_accidents (
                    uid TEXT PRIMARY KEY,
                    narrative TEXT,
                    date TEXT,
                    phase TEXT,
                    aircraft_type TEXT,
                    location TEXT,
                    operator TEXT
                )
                """
            )
        )
        await conn.execute(
            text(
                """
                INSERT INTO asn_scraped_accidents
                    (uid, narrative, date, phase, aircraft_type, location, operator)
                VALUES
                    ('asn_1', 'Test ASN narrative', '2024-02-02', 'approach',
                     'B737', 'Another City', 'Another Operator')
                """
            )
        )

        await conn.execute(
            text(
                """
                CREATE TABLE pci_scraped_accidents (
                    uid TEXT PRIMARY KEY,
                    summary TEXT,
                    date TEXT,
                    aircraft_type TEXT,
                    location TEXT,
                    operator TEXT
                )
                """
            )
        )

        await conn.execute(
            text(
                """
                CREATE TABLE human_evaluation (
                    id INTEGER PRIMARY KEY,
                    classification_result_id INTEGER,
                    evaluator_id TEXT,
                    human_category TEXT,
                    human_confidence REAL,
                    human_reasoning TEXT,
                    created_at TEXT
                )
                """
            )
        )

        await conn.execute(
            text(
                """
                CREATE TABLE evaluation_assignments (
                    id INTEGER PRIMARY KEY,
                    classification_result_id INTEGER,
                    evaluator_id TEXT,
                    is_complete BOOLEAN,
                    completed_at TEXT
                )
                """
            )
        )
        await conn.execute(
            text(
                """
                INSERT INTO evaluation_assignments
                    (classification_result_id, evaluator_id, is_complete, completed_at)
                VALUES
                    (101, 'test_evaluator', 0, NULL)
                """
            )
        )

    yield

    async with test_engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS evaluation_assignments"))
        await conn.execute(text("DROP TABLE IF EXISTS human_evaluation"))
        await conn.execute(text("DROP TABLE IF EXISTS pci_scraped_accidents"))
        await conn.execute(text("DROP TABLE IF EXISTS asn_scraped_accidents"))
        await conn.execute(text("DROP TABLE IF EXISTS asrs_records"))
        await conn.execute(text("DROP TABLE IF EXISTS classification_results"))
        await conn.execute(text("DROP TABLE IF EXISTS airport_location"))


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


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
    assert len(body) == 2
    source_uids = {row["source_uid"] for row in body}
    assert {"asrs_1", "asn_1"} <= source_uids


@pytest.mark.asyncio
async def test_get_full_classification_results_bulk(client):
    response = await client.post(
        "/full_classification_results_bulk", json=["asrs_1", "asn_1"]
    )
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
        "human_reasoning": "This is a test.",
    }

    response = await client.post("/human_evaluation/submit", json=payload)
    assert response.status_code == 200
    assert response.json() == {
        "status": "success",
        "message": "Evaluation submitted",
    }

    async with TestingSessionLocal() as session:
        res_eval = await session.execute(
            text(
                """
                SELECT classification_result_id, evaluator_id,
                       human_category, human_confidence, human_reasoning
                FROM human_evaluation
                WHERE classification_result_id = :c_id
                  AND evaluator_id = :e_id
                """
            ),
            {"c_id": 101, "e_id": "test_evaluator"},
        )
        row = res_eval.mappings().first()
        assert row is not None
        assert row["human_category"] == "Test Category"
        assert pytest.approx(row["human_confidence"], rel=1e-6) == 0.99
        assert row["human_reasoning"] == "This is a test."

        res_assign = await session.execute(
            text(
                """
                SELECT is_complete, completed_at
                FROM evaluation_assignments
                WHERE classification_result_id = :c_id
                  AND evaluator_id = :e_id
                """
            ),
            {"c_id": 101, "e_id": "test_evaluator"},
        )
        assign_row = res_assign.mappings().first()
        assert assign_row is not None
        assert assign_row["is_complete"] in (1, True)
        assert assign_row["completed_at"] is not None
