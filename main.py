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
async def get_classification_results(db: AsyncSession = Depends(get_db)) -> List[Dict[str, Any]]:
    query = text("SELECT * FROM classification_results")
    result = await db.execute(query)
    return [dict(row) for row in result.mappings().all()]


@app.post("/full_classification_results_bulk")
async def get_full_classification_results_bulk(
    uids: List[str], db: AsyncSession = Depends(get_db)
):
    if not uids:
        return {"results": {}, "aggregates": {}}

    asn_uids = [uid for uid in uids if uid.startswith("asn_")]
    asrs_uids = [uid for uid in uids if uid.startswith("asrs_")]
    pci_uids = [uid for uid in uids if uid.startswith("pci_")]

    results: List[Dict[str, Any]] = []

    async def fetch_rows(source_uids, origin_table, columns_map):
        if not source_uids:
            return []

        query = text(
            f"""
            SELECT cr.*, {columns_map}
            FROM classification_results cr
            JOIN {origin_table} origin ON origin.uid = cr.source_uid
            WHERE cr.source_uid IN :uids
            """
        ).bindparams(bindparam("uids", expanding=True))

        res = await db.execute(query, {"uids": source_uids})
        return [dict(row) for row in res.mappings().all()]

    results += await fetch_rows(
        asn_uids,
        "asn_scraped_accidents",
        "origin.uid AS origin_uid, origin.date AS origin_date, "
        "origin.phase AS origin_phase, origin.aircraft_type AS origin_aircraft_type, "
        "origin.location AS origin_location, origin.operator AS origin_operator, "
        "origin.narrative AS origin_narrative",
    )

    results += await fetch_rows(
        asrs_uids,
        "asrs_records",
        "origin.uid AS origin_uid, origin.time AS origin_date, "
        "origin.phase AS origin_phase, origin.aircraft_type AS origin_aircraft_type, "
        "origin.place AS origin_location, origin.operator AS origin_operator, "
        "origin.synopsis AS origin_narrative",
    )

    results += await fetch_rows(
        pci_uids,
        "pci_scraped_accidents",
        "origin.uid AS origin_uid, origin.date AS origin_date, "
        "NULL AS origin_phase, origin.aircraft_type AS origin_aircraft_type, "
        "origin.location AS origin_location, origin.operator AS origin_operator, "
        "origin.summary AS origin_narrative",
    )

    df = pd.DataFrame(results)
    aggregates: Dict[str, Any] = {}

    if not df.empty:
        aggregates = {
            "total_incidents": len(df),
            "unique_operators": df.get("origin_operator", pd.Series()).nunique(),
            "unique_aircraft_types": df.get("origin_aircraft_type", pd.Series()).nunique(),
            "phase_counts": df.get("origin_phase", pd.Series()).value_counts().to_dict(),
            "operator_counts": df.get("origin_operator", pd.Series()).value_counts().to_dict(),
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
    db: AsyncSession = Depends(get_db),
):
    """
    Inserts a record into public.human_evaluation and marks the assignment as complete.
    """

    now_ts = datetime.utcnow().isoformat()

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
        {
            "c_id": eval_req.classification_result_id,
            "e_id": eval_req.evaluator_id,
            "completed_at": now_ts,
        },
    )

    await db.commit()

    return {"status": "success", "message": "Evaluation submitted"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=58510, reload=True)
