from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Any, Dict, List, Optional
import calendar

# NOTE: The following imports assume you have a `database.py` for session
# management and a `models.py` file with an `Incident` table model.
# This has been updated to use raw SQL to align with the rest of the project.
from database import get_db

router = APIRouter(
    prefix="/aggregates",
    tags=["aggregates"],
)


@router.get(
    "/seasonal-distribution",
    response_model=List[dict],
    summary="Get seasonal distribution of incidents",
)
async def get_seasonal_distribution(
    start_year: Optional[int] = Query(
        None, description="The start year for the filter."
    ),
    end_year: Optional[int] = Query(None, description="The end year for the filter."),
    db: AsyncSession = Depends(get_db),
):
    """
    Provides aggregated incident counts by year and month for a seasonal heatmap
    visualization. The data is formatted for a Matrix chart.

    - **start_year**: Optional start year to filter the incidents.
    - **end_year**: Optional end year to filter the incidents.
    """
    params: Dict[str, Any] = {}
    where_clauses = ["origin_date IS NOT NULL"]

    if start_year is not None:
        where_clauses.append("EXTRACT(YEAR FROM origin_date) >= :start_year")
        params["start_year"] = start_year
    if end_year is not None:
        where_clauses.append("EXTRACT(YEAR FROM origin_date) <= :end_year")
        params["end_year"] = end_year
    
    where_sql = " AND ".join(where_clauses)

    query_str = f"""
        WITH all_incidents AS (
            SELECT sanitized_date AS origin_date FROM public.asn_scraped_accidents
            UNION ALL
            SELECT sanitized_date AS origin_date FROM public.asrs_records
            UNION ALL
            SELECT sanitized_date AS origin_date FROM public.pci_scraped_accidents
        )
        SELECT
            EXTRACT(YEAR FROM origin_date)::INTEGER AS year,
            EXTRACT(MONTH FROM origin_date)::INTEGER AS month,
            COUNT(*) AS count
        FROM all_incidents
        WHERE {where_sql}
        GROUP BY year, month
    """

    result = await db.execute(text(query_str), params)
    rows = result.all()

    data_map = {(row.year, row.month): row.count for row in rows}

    effective_start_year = start_year if start_year is not None else min((r.year for r in rows), default=None)
    effective_end_year = end_year if end_year is not None else max((r.year for r in rows), default=None)

    if effective_start_year is None or effective_end_year is None:
        return []

    response_data = []
    month_abbrs = [calendar.month_abbr[i] for i in range(1, 13)]
    for year in range(int(effective_start_year), int(effective_end_year) + 1):
        for month_num, month_abbr in enumerate(month_abbrs, 1):
            count = data_map.get((year, month_num), 0)
            response_data.append({"x": month_abbr, "y": str(year), "v": count})

    return response_data


@router.get(
    "/risk-heatmap",
    response_model=List[dict],
    summary="Get incident counts by phase and category for a risk heatmap.",
)
async def get_risk_heatmap(
    start_year: Optional[int] = Query(
        None, description="The start year for the filter (inclusive)."
    ),
    end_year: Optional[int] = Query(
        None, description="The end year for the filter (inclusive)."
    ),
    limit: int = Query(
        default=1000, gt=0, le=5000, description="Limit the number of results."
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Provides aggregated incident counts grouped by flight phase and final classification category,
    suitable for creating a risk matrix visualization.

    - **start_year**: Optional start year to filter incidents.
    - **end_year**: Optional end year to filter incidents.
    - **limit**: The maximum number of phase-category pairs to return.
    """
    params: Dict[str, Any] = {"limit": limit}
    where_clauses = ["inc.phase IS NOT NULL", "inc.final_category IS NOT NULL"]

    if start_year is not None:
        where_clauses.append("EXTRACT(YEAR FROM inc.origin_date) >= :start_year")
        params["start_year"] = start_year
    if end_year is not None:
        where_clauses.append("EXTRACT(YEAR FROM inc.origin_date) <= :end_year")
        params["end_year"] = end_year

    where_sql = " AND ".join(where_clauses)

    query_str = f"""
        WITH classified_incidents AS (
            SELECT cr.final_category, origin.sanitized_date AS origin_date, origin.phase
            FROM classification_results cr JOIN public.asn_scraped_accidents origin ON cr.source_uid = origin.uid
            UNION ALL
            SELECT cr.final_category, origin.sanitized_date AS origin_date, origin.phase
            FROM classification_results cr JOIN public.asrs_records origin ON cr.source_uid = origin.uid
            UNION ALL
            SELECT cr.final_category, origin.sanitized_date AS origin_date, NULL AS phase
            FROM classification_results cr JOIN public.pci_scraped_accidents origin ON cr.source_uid = origin.uid
        )
        SELECT
            inc.phase,
            inc.final_category AS category,
            COUNT(*) AS count
        FROM classified_incidents inc
        WHERE {where_sql}
        GROUP BY inc.phase, inc.final_category
        ORDER BY count DESC
        LIMIT :limit
    """

    result = await db.execute(text(query_str), params)
    return [dict(row) for row in result.mappings().all()]