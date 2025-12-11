from typing import Any, Dict, List, Literal
from datetime import datetime, date, timezone

import calendar
import pydantic
from fastapi import FastAPI, Depends, Query
from sqlalchemy import text, bindparam
from sqlalchemy.ext.asyncio import AsyncSession
import uvicorn
from database import get_db
import pandas as pd

import aggregates

app = FastAPI()

app.include_router(aggregates.router)

@app.get("/airports")
async def get_airports(
    codes: List[str] | None = Query(default=None),
    db: AsyncSession = Depends(get_db)
):
    if not codes:
        return {}

    codes_lower = [c.lower() for c in codes]

    query = text(
        """
        SELECT icao_code, iata_code, name, city, country, lat, lon
        FROM airport_location
        WHERE LOWER(icao_code) IN :codes
        """
    ).bindparams(bindparam("codes", expanding=True))

    result = await db.execute(query, {"codes": codes_lower})
    return {row["icao_code"]: dict(row) for row in result.mappings().all()}


@app.get("/classification-results")
async def get_classification_results(
    db: AsyncSession = Depends(get_db),
    skip: int = 0,
    limit: int = 100,
    evaluator_id: str | None = Query(default=None),
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"skip": skip, "limit": limit}
    where_clauses = []
    if evaluator_id:
        where_clauses.append("ea.evaluator_id = :evaluator_id")
        params["evaluator_id"] = str.upper(evaluator_id) 

    query_str = """
        SELECT
            cr.*,
            COALESCE(ea.is_complete, FALSE) AS is_complete,
            ea.evaluator_id
        FROM
            classification_results cr
        LEFT JOIN
            evaluation_assignments ea ON cr.id = ea.classification_result_id
    """
    if where_clauses:
        query_str += " WHERE " + " AND ".join(where_clauses)

    query_str += " ORDER BY cr.id OFFSET :skip LIMIT :limit"
    result = await db.execute(text(query_str), params)
    return [dict(row) for row in result.mappings().all()]


@app.post("/full_classification_results_bulk")
async def get_full_classification_results_bulk(
    uids: List[str], db: AsyncSession = Depends(get_db)
):
    if not uids:
        return {"results": {}, "aggregates": {}}
    
    # This single, powerful query replaces the three separate queries.
    # It uses UNION ALL to combine results from different source tables.
    # NOTE: This query is compatible with both PostgreSQL and SQLite.
    # The aggregation is still done in Python to maintain this compatibility.
    query = text("""
        SELECT
            cr.*,
            origin.uid AS origin_uid, origin.sanitized_date AS origin_date,
            origin.phase AS origin_phase, origin.aircraft_type AS origin_aircraft_type,
            origin.location AS origin_location, origin.operator AS origin_operator,
            origin.narrative AS origin_narrative
        FROM classification_results cr
        JOIN asn_scraped_accidents origin ON origin.uid = cr.source_uid
        WHERE cr.source_uid IN :uids

        UNION ALL

        SELECT
            cr.*,
            origin.uid AS origin_uid, origin.sanitized_date AS origin_date,
            origin.phase AS origin_phase, origin.aircraft_type AS origin_aircraft_type,
            origin.place AS origin_location, origin.operator AS origin_operator,
            origin.synopsis AS origin_narrative
        FROM classification_results cr
        JOIN asrs_records origin ON origin.uid = cr.source_uid
        WHERE cr.source_uid IN :uids
    
        UNION ALL
    
        SELECT
            cr.*,
            origin.uid AS origin_uid, origin.sanitized_date AS origin_date,
            NULL AS origin_phase, origin.aircraft_type AS origin_aircraft_type,
            origin.location AS origin_location, origin.operator AS origin_operator,
            origin.summary AS origin_narrative
        FROM classification_results cr
        JOIN pci_scraped_accidents origin ON origin.uid = cr.source_uid
        WHERE cr.source_uid IN :uids
    """).bindparams(bindparam("uids", expanding=True))

    result = await db.execute(query, {"uids": tuple(uids)})
    results = [dict(row) for row in result.mappings().all()]

    # Aggregation is still performed in pandas to maintain compatibility with SQLite tests
    aggregates = {}
    if results:
        df = pd.DataFrame(results)
        aggregates = {
            "total_incidents": len(df),
            "unique_operators": df["origin_operator"].nunique(),
            "unique_aircraft_types": df["origin_aircraft_type"].nunique(),
            "phase_counts": df["origin_phase"].value_counts().to_dict(),
            "operator_counts": df["origin_operator"].value_counts().to_dict(),
        }
        
    return {"results": {row["source_uid"]: row for row in results}, "aggregates": aggregates}


@app.get("/incidents/classified-detailed")
async def get_classified_incidents_with_details(
    skip: int = Query(default=0, ge=0, description="Number of records to skip for pagination."),
    limit: int = Query(default=10, gt=0, le=100, description="Maximum number of records to return."),
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieves a paginated list of the most recent classified incidents, including
    key details like date, operator, phase, aircraft type, and final classification.
    """
    query = text("""
        WITH all_classified_incidents AS (
            SELECT
                cr.id, cr.source_uid, cr.final_category, cr.final_confidence,
                origin.sanitized_date AS origin_date,
                origin.phase AS origin_phase,
                origin.aircraft_type AS origin_aircraft_type,
                origin.operator AS origin_operator
            FROM classification_results cr
            JOIN asn_scraped_accidents origin ON origin.uid = cr.source_uid

            UNION ALL

            SELECT
                cr.id, cr.source_uid, cr.final_category, cr.final_confidence,
                origin.sanitized_date AS origin_date,
                origin.phase AS origin_phase,
                origin.aircraft_type AS origin_aircraft_type,
                origin.operator AS origin_operator
            FROM classification_results cr
            JOIN asrs_records origin ON origin.uid = cr.source_uid

            UNION ALL

            SELECT
                cr.id, cr.source_uid, cr.final_category, cr.final_confidence,
                origin.sanitized_date AS origin_date,
                NULL AS origin_phase,
                origin.aircraft_type AS origin_aircraft_type,
                origin.operator AS origin_operator
            FROM classification_results cr
            JOIN pci_scraped_accidents origin ON cr.source_uid = origin.uid
        )
        SELECT *
        FROM all_classified_incidents
        WHERE origin_date IS NOT NULL
        ORDER BY origin_date DESC
        OFFSET :skip
        LIMIT :limit
    """)

    params = {"skip": skip, "limit": limit}
    result = await db.execute(query, params)
    return [dict(row) for row in result.mappings().all()]


@app.get("/aggregates/over-time")
async def get_aggregates_over_time(
    period: Literal["year", "month"] = Query(
        default="month", description="The time period to group by ('year' or 'month')."
    ),
    operators: List[str] | None = Query(default=None, description="Filter by one or more operators."),
    phases: List[str] | None = Query(default=None, description="Filter by one or more flight phases."),
    aircraft_types: List[str] | None = Query(default=None, description="Filter by one or more aircraft types."),
    locations: List[str] | None = Query(default=None, description="Filter by one or more locations (ICAO codes)."),
    start_period: str | None = Query(default=None, description="Start period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    end_period: str | None = Query(default=None, description="End period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Provides aggregated incident counts over time (by year or month).
    This is useful for creating time-series visualizations.
    
    The endpoint supports filtering by:
    - `operators`
    - `phases`
    - `aircraft_types`
    """
    if period == "year":
        # Note: The date extraction function might vary between SQL dialects.
        # EXTRACT(YEAR FROM ...) is standard SQL.
        date_trunc_sql = "EXTRACT(YEAR FROM origin_date)"
    else:  # month
        # Use TO_CHAR for PostgreSQL to format date as 'YYYY-MM'.
        date_trunc_sql = "TO_CHAR(origin_date, 'YYYY-MM')"

    params: Dict[str, Any] = {}
    where_clauses = ["origin_date IS NOT NULL"]

    if operators:
        where_clauses.append("operator IN :operators")
        params["operators"] = tuple(operators)
    if phases:
        where_clauses.append("phase IN :phases")
        params["phases"] = tuple(phases)
    if aircraft_types:
        where_clauses.append("aircraft_type IN :aircraft_types")
        params["aircraft_types"] = tuple(aircraft_types)
    if start_period:
        year, month = map(int, start_period.split('-'))
        start_date = date(year, month, 1)
        where_clauses.append("origin_date >= :start_date")
        params["start_date"] = start_date
    if end_period:
        year, month = map(int, end_period.split('-'))
        _, last_day = calendar.monthrange(year, month)
        end_date = date(year, month, last_day)
        where_clauses.append("origin_date <= :end_date")
        params["end_date"] = end_date

    where_sql = " AND ".join(where_clauses)

    # This query unions the dates from the different source tables
    # before performing the aggregation.
    query = text(f"""
        WITH all_incidents AS (
            SELECT sanitized_date AS origin_date, operator, phase, aircraft_type
            FROM public.asn_scraped_accidents
            UNION ALL
            SELECT sanitized_date AS origin_date, operator, phase, aircraft_type
            FROM public.asrs_records
            UNION ALL
            SELECT sanitized_date AS origin_date, operator, NULL AS phase, aircraft_type
            FROM public.pci_scraped_accidents
        )
        SELECT
            {date_trunc_sql} AS period_start,
            COUNT(*) AS incident_count
        FROM all_incidents
        WHERE {where_sql}
        GROUP BY period_start
        ORDER BY period_start;
    """)
    result = await db.execute(query, params)
    return [dict(row) for row in result.mappings().all()]


@app.get("/aggregates/top-n")
async def get_top_n_aggregates(
    category: Literal["operator", "aircraft_type", "phase", "final_category", "location"] = Query(
        ..., description="The category to aggregate."
    ),
    n: int = Query(default=10, gt=0, le=100, description="The number of top results to return."),
    operators: List[str] | None = Query(default=None, description="Filter by one or more operators."),
    phases: List[str] | None = Query(default=None, description="Filter by one or more flight phases."),
    aircraft_types: List[str] | None = Query(default=None, description="Filter by one or more aircraft types."),
    locations: List[str] | None = Query(default=None, description="Filter by one or more locations (ICAO codes)."),
    start_period: str | None = Query(default=None, description="Start period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    end_period: str | None = Query(default=None, description="End period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Provides a list of the top N most frequent items for a given category,
    such as 'operator', 'aircraft_type', 'phase', or 'final_category'.
    """
    category_map = {
        "operator": "operator",
        "aircraft_type": "aircraft_type",
        "phase": "phase",
        "location": "location",
        "final_category": "final_category",
    }
    # FastAPI's Literal validation makes this check mostly redundant, but it's a safe fallback.
    if category not in category_map:
        return []
    group_by_col = category_map[category]

    params: Dict[str, Any] = {"n": n}
    where_clauses = [f"{group_by_col} IS NOT NULL", f"{group_by_col} != ''"]

    if operators:
        where_clauses.append("operator IN :operators")
        params["operators"] = tuple(operators)
    if phases:
        where_clauses.append("phase IN :phases")
        params["phases"] = tuple(phases)
    if aircraft_types:
        where_clauses.append("aircraft_type IN :aircraft_types")
        params["aircraft_types"] = tuple(aircraft_types)
    if locations:
        where_clauses.append("location IN :locations")
        params["locations"] = tuple(locations)
    if start_period:
        year, month = map(int, start_period.split('-'))
        start_date = date(year, month, 1)
        where_clauses.append("origin_date >= :start_date")
        params["start_date"] = start_date
    if end_period:
        year, month = map(int, end_period.split('-'))
        _, last_day = calendar.monthrange(year, month)
        end_date = date(year, month, last_day)
        where_clauses.append("origin_date <= :end_date")
        params["end_date"] = end_date

    where_sql = " AND ".join(where_clauses)

    # The CTE needs to change based on whether we are aggregating a classification category
    # or a raw incident attribute.
    if category == "final_category":
        cte_sql = """
            WITH all_incidents AS (
                SELECT cr.final_category, origin.operator, origin.sanitized_date AS origin_date, origin.phase, origin.aircraft_type, origin.location
                FROM classification_results cr JOIN public.asn_scraped_accidents origin ON cr.source_uid = origin.uid
                UNION ALL
                SELECT cr.final_category, origin.operator, origin.sanitized_date AS origin_date, origin.phase, origin.aircraft_type, origin.place AS location
                FROM classification_results cr JOIN public.asrs_records origin ON cr.source_uid = origin.uid
                UNION ALL
                SELECT cr.final_category, origin.operator, origin.sanitized_date AS origin_date, NULL AS phase, origin.aircraft_type, origin.location
                FROM classification_results cr JOIN public.pci_scraped_accidents origin ON cr.source_uid = origin.uid
            )
        """
    else:
        cte_sql = """
            WITH all_incidents AS (
                SELECT sanitized_date AS origin_date, operator, phase, aircraft_type, location FROM asn_scraped_accidents
                UNION ALL
                SELECT sanitized_date AS origin_date, operator, phase, aircraft_type, place AS location FROM asrs_records
                UNION ALL
                SELECT sanitized_date AS origin_date, operator, NULL AS phase, aircraft_type, location FROM pci_scraped_accidents
            )
        """

    query = text(f"""
        {cte_sql}
        SELECT
            {group_by_col} AS category_value,
            COUNT(*) AS incident_count
        FROM all_incidents
        WHERE {where_sql}
        GROUP BY {group_by_col}
        ORDER BY incident_count DESC
        LIMIT :n;
    """)

    result = await db.execute(query, params)
    return [dict(row) for row in result.mappings().all()]


@app.get("/aggregates/classification-over-time")
async def get_classification_aggregates_over_time(
    period: Literal["year", "month"] = Query(
        default="month", description="The time period to group by ('year' or 'month')."
    ),
    final_categories: List[str] | None = Query(default=None, description="Filter by one or more final classification categories."),
    phases: List[str] | None = Query(default=None, description="Filter by one or more flight phases."),
    locations: List[str] | None = Query(default=None, description="Filter by one or more locations (ICAO codes)."),
    aircraft_types: List[str] | None = Query(default=None, description="Filter by one or more aircraft types."),
    start_period: str | None = Query(default=None, description="Start period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    end_period: str | None = Query(default=None, description="End period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Provides aggregated time-series data for classification results,
    supporting filtering by classification, phase, location, and aircraft type.
    """
    if period == "year":
        date_trunc_sql = "EXTRACT(YEAR FROM inc.origin_date)"
    else:  # month
        date_trunc_sql = "TO_CHAR(inc.origin_date, 'YYYY-MM')"

    params: Dict[str, Any] = {}
    where_clauses = ["inc.origin_date IS NOT NULL"]

    if final_categories:
        where_clauses.append("inc.final_category IN :final_categories")
        params["final_categories"] = tuple(final_categories)
    if phases:
        where_clauses.append("inc.phase IN :phases")
        params["phases"] = tuple(phases)
    if locations:
        where_clauses.append("inc.location IN :locations")
        params["locations"] = tuple(locations)
    if aircraft_types:
        where_clauses.append("inc.aircraft_type IN :aircraft_types")
        params["aircraft_types"] = tuple(aircraft_types)
    if locations:
        where_clauses.append("inc.location IN :locations")
        params["locations"] = tuple(locations)
    if start_period:
        year, month = map(int, start_period.split('-'))
        start_date = date(year, month, 1)
        where_clauses.append("inc.origin_date >= :start_date")
        params["start_date"] = start_date
    if end_period:
        year, month = map(int, end_period.split('-'))
        _, last_day = calendar.monthrange(year, month)
        end_date = date(year, month, last_day)
        where_clauses.append("inc.origin_date <= :end_date")
        params["end_date"] = end_date

    where_sql = " AND ".join(where_clauses)

    query = text(f"""
        WITH classified_incidents AS (
            SELECT cr.final_category, origin.sanitized_date AS origin_date, origin.phase, origin.aircraft_type, origin.location
            FROM classification_results cr JOIN asn_scraped_accidents origin ON cr.source_uid = origin.uid
            UNION ALL
            SELECT cr.final_category, origin.sanitized_date AS origin_date, origin.phase, origin.aircraft_type, origin.place AS location
            FROM classification_results cr JOIN asrs_records origin ON cr.source_uid = origin.uid
            UNION ALL
            SELECT cr.final_category, origin.sanitized_date AS origin_date, NULL AS phase, origin.aircraft_type, origin.location
            FROM classification_results cr JOIN pci_scraped_accidents origin ON cr.source_uid = origin.uid
        )
        SELECT
            {date_trunc_sql} AS period_start,
            COUNT(*) AS incident_count
        FROM classified_incidents inc
        WHERE {where_sql}
        GROUP BY period_start
        ORDER BY period_start;
    """)
    result = await db.execute(query, params)
    return [dict(row) for row in result.mappings().all()]


@app.get("/incidents/locations")
async def get_incident_locations(
    operators: List[str] | None = Query(default=None, description="Filter by one or more operators."),
    phases: List[str] | None = Query(default=None, description="Filter by one or more flight phases."),
    aircraft_types: List[str] | None = Query(default=None, description="Filter by one or more aircraft types."),
    start_period: str | None = Query(default=None, description="Start period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    end_period: str | None = Query(default=None, description="End period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Provides a list of incidents with their geographic coordinates, suitable for map visualizations.
    It supports filtering by a date range.
    """
    params: Dict[str, Any] = {}
    where_clauses = ["al.lat IS NOT NULL AND al.lon IS NOT NULL"]

    if operators:
        where_clauses.append("inc.operator IN :operators")
        params["operators"] = tuple(operators)
    if phases:
        where_clauses.append("inc.phase IN :phases")
        params["phases"] = tuple(phases)
    if aircraft_types:
        where_clauses.append("inc.aircraft_type IN :aircraft_types")
        params["aircraft_types"] = tuple(aircraft_types)
    if start_period:
        year, month = map(int, start_period.split('-'))
        start_date = date(year, month, 1)
        where_clauses.append("inc.origin_date >= :start_date")
        params["start_date"] = start_date
    if end_period:
        year, month = map(int, end_period.split('-'))
        _, last_day = calendar.monthrange(year, month)
        end_date = date(year, month, last_day)
        where_clauses.append("inc.origin_date <= :end_date")
        params["end_date"] = end_date

    where_sql = " AND ".join(where_clauses)

    query = text(f"""
        WITH all_incidents AS (
            SELECT uid, narrative AS summary, sanitized_date AS origin_date, phase, aircraft_type,
                location, operator
            FROM asn_scraped_accidents
            UNION ALL
            SELECT uid, synopsis AS summary, sanitized_date AS origin_date, phase, aircraft_type,
                place AS location, operator
            FROM asrs_records
            UNION ALL
            SELECT uid, summary, sanitized_date AS origin_date, NULL as phase, aircraft_type,
                location, operator
            FROM pci_scraped_accidents
        )
        SELECT
            inc.uid, inc.summary, inc.origin_date, inc.operator,
            al.lat, al.lon, al.name as location_name
        FROM all_incidents inc
        LEFT JOIN airport_location al ON inc.location = al.icao_code
        WHERE {where_sql}
        ORDER BY inc.origin_date DESC;
    """)

    result = await db.execute(query, params)
    return [dict(row) for row in result.mappings().all()]


@app.get("/aggregates/hierarchy")
async def get_hierarchy_aggregates(
    operators: List[str] | None = Query(default=None, description="Filter by one or more operators."),
    phases: List[str] | None = Query(default=None, description="Filter by one or more flight phases."),
    aircraft_types: List[str] | None = Query(default=None, description="Filter by one or more aircraft types."),
    locations: List[str] | None = Query(default=None, description="Filter by one or more locations (ICAO codes)."),
    start_period: str | None = Query(default=None, description="Start period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    end_period: str | None = Query(default=None, description="End period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Provides data grouped by operator, aircraft_type, and phase, suitable for
    hierarchical visualizations like sunburst or treemap charts.
    """
    params: Dict[str, Any] = {}
    where_clauses = ["operator IS NOT NULL", "aircraft_type IS NOT NULL", "phase IS NOT NULL"]

    if operators:
        where_clauses.append("operator IN :operators")
        params["operators"] = tuple(operators)
    if phases:
        where_clauses.append("phase IN :phases")
        params["phases"] = tuple(phases)
    if aircraft_types:
        where_clauses.append("aircraft_type IN :aircraft_types")
        params["aircraft_types"] = tuple(aircraft_types)
    if locations:
        where_clauses.append("location IN :locations")
        params["locations"] = tuple(locations)
    if start_period:
        year, month = map(int, start_period.split('-'))
        start_date = date(year, month, 1)
        where_clauses.append("origin_date >= :start_date")
        params["start_date"] = start_date
    if end_period:
        year, month = map(int, end_period.split('-'))
        _, last_day = calendar.monthrange(year, month)
        end_date = date(year, month, last_day)
        where_clauses.append("origin_date <= :end_date")
        params["end_date"] = end_date

    where_sql = " AND ".join(where_clauses)

    query = text(f"""
        WITH all_incidents AS (
            SELECT sanitized_date AS origin_date, operator, phase, aircraft_type, location FROM asn_scraped_accidents
            UNION ALL
            SELECT sanitized_date AS origin_date, operator, phase, aircraft_type, place as location FROM asrs_records
            UNION ALL
            SELECT sanitized_date AS origin_date, operator, NULL as phase, aircraft_type, location FROM pci_scraped_accidents
        )
        SELECT operator, aircraft_type, phase, COUNT(*) as incident_count
        FROM all_incidents
        WHERE {where_sql}
        GROUP BY operator, aircraft_type, phase;
    """)
    result = await db.execute(query, params)
    return [dict(row) for row in result.mappings().all()]


@app.get("/aggregates/locations-over-time")
async def get_locations_over_time_aggregates(
    operators: List[str] | None = Query(default=None, description="Filter by one or more operators."),
    phases: List[str] | None = Query(default=None, description="Filter by one or more flight phases."),
    aircraft_types: List[str] | None = Query(default=None, description="Filter by one or more aircraft types."),
    locations: List[str] | None = Query(default=None, description="Filter by one or more locations (ICAO codes)."),
    start_period: str | None = Query(default=None, description="Start period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    end_period: str | None = Query(default=None, description="End period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Provides aggregated incident counts per location, grouped by month.
    This is a more performant alternative to /incidents/locations for heatmap-style time-series visualizations.
    """
    params: Dict[str, Any] = {}
    where_clauses = ["inc.location IS NOT NULL", "inc.origin_date IS NOT NULL"]

    if operators:
        where_clauses.append("inc.operator IN :operators")
        params["operators"] = tuple(operators)
    if phases:
        where_clauses.append("inc.phase IN :phases")
        params["phases"] = tuple(phases)
    if aircraft_types:
        where_clauses.append("inc.aircraft_type IN :aircraft_types")
        params["aircraft_types"] = tuple(aircraft_types)
    if locations:
        # Use LOWER for case-insensitive matching on the location codes
        where_clauses.append("LOWER(inc.location) IN :locations")
        params["locations"] = tuple(l.lower() for l in locations)
    if start_period:
        year, month = map(int, start_period.split('-'))
        start_date = date(year, month, 1)
        where_clauses.append("inc.origin_date >= :start_date")
        params["start_date"] = start_date
    if end_period:
        year, month = map(int, end_period.split('-'))
        _, last_day = calendar.monthrange(year, month)
        end_date = date(year, month, last_day)
        where_clauses.append("inc.origin_date <= :end_date")
        params["end_date"] = end_date

    where_sql = " AND ".join(where_clauses)

    query = text(f"""
        WITH all_incidents AS (
            SELECT sanitized_date AS origin_date, operator, phase, aircraft_type, location FROM asn_scraped_accidents
            UNION ALL
            SELECT sanitized_date AS origin_date, operator, phase, aircraft_type, place as location FROM asrs_records
            UNION ALL
            SELECT sanitized_date AS origin_date, operator, NULL as phase, aircraft_type, location FROM pci_scraped_accidents
        )
        SELECT
            al.lat, al.lon, al.name AS location_name,
            TO_CHAR(inc.origin_date, 'YYYY-MM') AS period,
            COUNT(*) AS incident_count
        FROM all_incidents inc
        JOIN airport_location al ON LOWER(inc.location) = LOWER(al.icao_code)
        WHERE {where_sql}
        GROUP BY al.lat, al.lon, al.name, period
        ORDER BY period, incident_count DESC;
    """)
    result = await db.execute(query, params)
    return [dict(row) for row in result.mappings().all()]


@app.get("/aggregates/by-location")
async def get_aggregates_by_location(
    operators: List[str] | None = Query(default=None, description="Filter by one or more operators."),
    phases: List[str] | None = Query(default=None, description="Filter by one or more flight phases."),
    aircraft_types: List[str] | None = Query(default=None, description="Filter by one or more aircraft types."),
    start_period: str | None = Query(default=None, description="Start period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    end_period: str | None = Query(default=None, description="End period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Provides a count of incidents for each location, supporting time range and other filters.
    """
    params: Dict[str, Any] = {}
    where_clauses = ["location IS NOT NULL", "location != ''"]

    if operators:
        where_clauses.append("operator IN :operators")
        params["operators"] = tuple(operators)
    if phases:
        where_clauses.append("phase IN :phases")
        params["phases"] = tuple(phases)
    if aircraft_types:
        where_clauses.append("aircraft_type IN :aircraft_types")
        params["aircraft_types"] = tuple(aircraft_types)
    if start_period:
        year, month = map(int, start_period.split('-'))
        start_date = date(year, month, 1)
        where_clauses.append("origin_date >= :start_date")
        params["start_date"] = start_date
    if end_period:
        year, month = map(int, end_period.split('-'))
        _, last_day = calendar.monthrange(year, month)
        end_date = date(year, month, last_day)
        where_clauses.append("origin_date <= :end_date")
        params["end_date"] = end_date

    where_sql = " AND ".join(where_clauses)

    query = text(f"""
        WITH all_incidents AS (
            SELECT sanitized_date AS origin_date, operator, phase, aircraft_type, location FROM asn_scraped_accidents
            UNION ALL
            SELECT sanitized_date AS origin_date, operator, phase, aircraft_type, place as location FROM asrs_records
            UNION ALL
            SELECT sanitized_date AS origin_date, operator, NULL as phase, aircraft_type, location FROM pci_scraped_accidents
        )
        SELECT location, COUNT(*) as incident_count
        FROM all_incidents
        WHERE {where_sql}
        GROUP BY location
        ORDER BY incident_count DESC;
    """)
    result = await db.execute(query, params)
    return [dict(row) for row in result.mappings().all()]


@app.get("/aggregates/heatmap")
async def get_heatmap_aggregates(
    dimension1: Literal["operator", "aircraft_type", "phase"] = Query(..., description="The first dimension for the heatmap."),
    dimension2: Literal["operator", "aircraft_type", "phase"] = Query(..., description="The second dimension for the heatmap."),
    operators: List[str] | None = Query(default=None, description="Filter by one or more operators."),
    phases: List[str] | None = Query(default=None, description="Filter by one or more flight phases."),
    aircraft_types: List[str] | None = Query(default=None, description="Filter by one or more aircraft types."),
    locations: List[str] | None = Query(default=None, description="Filter by one or more locations (ICAO codes)."),
    start_period: str | None = Query(default=None, description="Start period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    end_period: str | None = Query(default=None, description="End period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Provides aggregated data suitable for a heatmap visualization, showing the
    relationship between two categorical dimensions.
    """
    if dimension1 == dimension2:
        return []

    col_map = {
        "operator": "operator",
        "aircraft_type": "aircraft_type",
        "phase": "phase",
    }
    dim1_col = col_map[dimension1]
    dim2_col = col_map[dimension2]

    params: Dict[str, Any] = {}
    where_clauses = [f"{dim1_col} IS NOT NULL", f"{dim2_col} IS NOT NULL"]

    if operators:
        where_clauses.append("operator IN :operators")
        params["operators"] = tuple(operators)
    if phases:
        where_clauses.append("phase IN :phases")
        params["phases"] = tuple(phases)
    if aircraft_types:
        where_clauses.append("aircraft_type IN :aircraft_types")
        params["aircraft_types"] = tuple(aircraft_types)
    if locations:
        where_clauses.append("location IN :locations")
        params["locations"] = tuple(locations)
    if start_period:
        year, month = map(int, start_period.split('-'))
        start_date = date(year, month, 1)
        where_clauses.append("origin_date >= :start_date")
        params["start_date"] = start_date
    if end_period:
        year, month = map(int, end_period.split('-'))
        _, last_day = calendar.monthrange(year, month)
        end_date = date(year, month, last_day)
        where_clauses.append("origin_date <= :end_date")
        params["end_date"] = end_date

    where_sql = " AND ".join(where_clauses)

    query = text(f"""
        WITH all_incidents AS (
            SELECT sanitized_date AS origin_date, operator, phase, aircraft_type, location FROM asn_scraped_accidents
            UNION ALL
            SELECT sanitized_date AS origin_date, operator, phase, aircraft_type, place as location FROM asrs_records
            UNION ALL
            SELECT sanitized_date AS origin_date, operator, NULL AS phase, aircraft_type, location FROM pci_scraped_accidents
        )
        SELECT
            {dim1_col} AS dim1_value,
            {dim2_col} AS dim2_value,
            COUNT(*) AS incident_count
        FROM all_incidents
        WHERE {where_sql}
        GROUP BY {dim1_col}, {dim2_col}
        ORDER BY incident_count DESC;
    """)

    result = await db.execute(query, params)
    return [dict(row) for row in result.mappings().all()]


@app.get("/aggregates/statistics")
async def get_statistics(
    operators: List[str] | None = Query(default=None, description="Filter by one or more operators."),
    phases: List[str] | None = Query(default=None, description="Filter by one or more flight phases."),
    aircraft_types: List[str] | None = Query(default=None, description="Filter by one or more aircraft types."),
    locations: List[str] | None = Query(default=None, description="Filter by one or more locations (ICAO codes)."),
    start_period: str | None = Query(default=None, description="Start period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    end_period: str | None = Query(default=None, description="End period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    db: AsyncSession = Depends(get_db)
):
    """
    Provides high-level summary statistics, including the total number of incidents.
    """
    params: Dict[str, Any] = {}
    where_clauses = ["uid IS NOT NULL"]

    if operators:
        where_clauses.append("operator IN :operators")
        params["operators"] = tuple(operators)
    if phases:
        where_clauses.append("phase IN :phases")
        params["phases"] = tuple(phases)
    if aircraft_types:
        where_clauses.append("aircraft_type IN :aircraft_types")
        params["aircraft_types"] = tuple(aircraft_types)
    if locations:
        where_clauses.append("location IN :locations")
        params["locations"] = tuple(locations)
    if start_period:
        year, month = map(int, start_period.split('-'))
        start_date = date(year, month, 1)
        where_clauses.append("sanitized_date >= :start_date")
        params["start_date"] = start_date
    if end_period:
        year, month = map(int, end_period.split('-'))
        _, last_day = calendar.monthrange(year, month)
        end_date = date(year, month, last_day)
        where_clauses.append("sanitized_date <= :end_date")
        params["end_date"] = end_date

    where_sql = " AND ".join(where_clauses)

    query = text(f"""
        WITH all_incidents AS (
            SELECT uid, sanitized_date, operator, phase, aircraft_type, location FROM public.asn_scraped_accidents
            UNION ALL
            SELECT uid, sanitized_date, operator, phase, aircraft_type, place as location FROM public.asrs_records
            UNION ALL
            SELECT uid, sanitized_date, operator, NULL as phase, aircraft_type, location FROM public.pci_scraped_accidents
        )
        SELECT COUNT(*) as total_incidents FROM all_incidents WHERE {where_sql};
    """)

    result = await db.execute(query, params)
    stats = result.mappings().first()

    if not stats:
        return {"total_incidents": 0}

    return dict(stats)


# -------------------------------------------------------------------
# Human Evaluation Endpoint
# -------------------------------------------------------------------

class HumanEvaluationRequest(pydantic.BaseModel):
    classification_result_id: int
    evaluator_id: str
    human_category: str
    human_confidence: float
    human_reasoning: str


@app.post("/human_evaluation/submit")
async def submit_human_evaluation(
    eval_req: HumanEvaluationRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Inserts a record into public.human_evaluation and marks the assignment as complete.
    """

    now_ts = datetime.now(timezone.utc)

    try:
        # Check if the assignment exists and is not complete
        assignment_check_query = text("""
            SELECT assignment_id FROM evaluation_assignments -- lock the row for update
            WHERE classification_result_id = :c_id
              AND evaluator_id = :e_id
              AND is_complete = FALSE
        """)
        result = await db.execute(assignment_check_query, {"c_id": eval_req.classification_result_id, "e_id": eval_req.evaluator_id}) # type: ignore
        if result.first() is None:
            return {"status": "error", "message": "Assignment not found or already complete."}

        insert_query = text(
            """
            INSERT INTO human_evaluation
            (classification_result_id, evaluator_id, human_category,
             human_confidence, human_reasoning, created_at)
            VALUES
            (:c_id, :e_id, :h_cat, :h_conf, :h_reason, :created_at)
            """
        )
        await db.execute(
            insert_query,
            {
                "c_id": eval_req.classification_result_id,
                "e_id": eval_req.evaluator_id,
                "h_cat": eval_req.human_category,
                "h_conf": eval_req.human_confidence,
                "h_reason": eval_req.human_reasoning,
                "created_at": now_ts,
            },
        )

        update_query = text(
            """
            UPDATE evaluation_assignments
            SET is_complete = TRUE, completed_at = :completed_at
            WHERE classification_result_id = :c_id AND evaluator_id = :e_id
            """
        )
        await db.execute(
            update_query,
            {"c_id": eval_req.classification_result_id, "e_id": eval_req.evaluator_id, "completed_at": now_ts},
        )

        await db.commit()
    except Exception:
        await db.rollback()
        raise

    return {"status": "success", "message": "Evaluation submitted"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=58510, reload=True)
