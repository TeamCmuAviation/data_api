from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Any, Dict, List, Optional
from datetime import date
import calendar

from database import get_db

router = APIRouter(
    prefix="/reports",
    tags=["reports"],
)


@router.get("/uids_by_filter", response_model=List[str])
async def get_uids_by_filter(
    operators: Optional[List[str]] = Query(default=None, description="Filter by one or more operators."),
    locations: Optional[List[str]] = Query(default=None, description="Filter by one or more locations (ICAO codes)."),
    phases: Optional[List[str]] = Query(default=None, description="Filter by one or more flight phases."),
    aircraft_types: Optional[List[str]] = Query(default=None, description="Filter by one or more aircraft types."),
    start_period: Optional[str] = Query(default=None, description="Start period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    end_period: Optional[str] = Query(default=None, description="End period in YYYY-MM format.", regex=r"^\d{4}-\d{2}$"),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetches a list of incident UIDs based on a set of optional filters.
    This is useful for getting a list of incidents to then pass to other endpoints
    like `/full_classification_results_bulk`.
    """
    params: Dict[str, Any] = {}
    where_clauses = ["uid IS NOT NULL"]

    if operators:
        where_clauses.append("operator IN :operators")
        params["operators"] = tuple(operators)
    if locations:
        where_clauses.append("location IN :locations")
        params["locations"] = tuple(locations)
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

    query_str = f"""
        WITH all_incidents AS (
            SELECT uid, sanitized_date AS origin_date, operator, phase, aircraft_type, location FROM public.asn_scraped_accidents
            UNION ALL
            SELECT uid, sanitized_date AS origin_date, operator, phase, aircraft_type, place AS location FROM public.asrs_records
            UNION ALL
            SELECT uid, sanitized_date AS origin_date, operator, NULL AS phase, aircraft_type, location FROM public.pci_scraped_accidents
        )
        SELECT uid FROM all_incidents WHERE {where_sql} ORDER BY origin_date DESC;
    """

    query = text(query_str)
    if operators:
        query = query.bindparams(bindparam("operators", expanding=True))
    if locations:
        query = query.bindparams(bindparam("locations", expanding=True))
    if phases:
        query = query.bindparams(bindparam("phases", expanding=True))
    if aircraft_types:
        query = query.bindparams(bindparam("aircraft_types", expanding=True))
    result = await db.execute(query, params)
    return [row[0] for row in result.all()]