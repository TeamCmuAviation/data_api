from typing import Any, Dict, List
from datetime import datetime

import pydantic
from fastapi import FastAPI, Depends, Query
from sqlalchemy import text, bindparam
from sqlalchemy.ext.asyncio import AsyncSession
import uvicorn
from database import get_db
import pandas as pd

app = FastAPI()


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
            origin.uid AS origin_uid, origin.date AS origin_date,
            origin.phase AS origin_phase, origin.aircraft_type AS origin_aircraft_type,
            origin.location AS origin_location, origin.operator AS origin_operator,
            origin.narrative AS origin_narrative
        FROM classification_results cr
        JOIN asn_scraped_accidents origin ON origin.uid = cr.source_uid
        WHERE cr.source_uid IN :uids
    
        UNION ALL
    
        SELECT
            cr.*,
            origin.uid AS origin_uid, origin.time AS origin_date,
            origin.phase AS origin_phase, origin.aircraft_type AS origin_aircraft_type,
            origin.place AS origin_location, origin.operator AS origin_operator,
            origin.synopsis AS origin_narrative
        FROM classification_results cr
        JOIN asrs_records origin ON origin.uid = cr.source_uid
        WHERE cr.source_uid IN :uids
    
        UNION ALL
    
        SELECT
            cr.*,
            origin.uid AS origin_uid, origin.date AS origin_date,
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

    now_ts = datetime.utcnow()

    try:
        # Check if the assignment exists and is not complete
        assignment_check_query = text("""
            SELECT assignment_id FROM evaluation_assignments -- lock the row for update
            WHERE classification_result_id = :c_id
              AND evaluator_id = :e_id
              AND is_complete = FALSE
            FOR UPDATE
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
