from typing import Any, Dict, List

from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db

app = FastAPI()


@app.get("/record/{uid}")
async def get_record(uid: str, db: AsyncSession = Depends(get_db)) -> Dict[str, Any]:
    """
    Single GET endpoint that:
    - Infers the table to query based on the prefix before the first '_' in the UID.
      * 'asn_'  -> public.asn_scraped_accidents
      * 'asrs_' -> public.asrs_records
      * 'pci_'  -> public.pci_scraped_accidents
    - Returns a normalized JSON object with:
      uid, date, phase (if available), aircraft_type, location, operator, narrative
    """
    prefix = uid.split("_", 1)[0]

    if prefix == "asn":
        query = text(
            """
            SELECT
                uid,
                date,
                phase,
                aircraft_type,
                location,
                operator,
                narrative
            FROM public.asn_scraped_accidents
            WHERE uid = :uid
            """
        )
    elif prefix == "asrs":
        query = text(
            """
            SELECT
                uid,
                time AS date,
                phase,
                aircraft_type,
                place AS location,
                operator,
                synopsis AS narrative
            FROM public.asrs_records
            WHERE uid = :uid
            """
        )
    elif prefix == "pci":
        query = text(
            """
            SELECT
                uid,
                date,
                NULL AS phase,
                ac_________type AS aircraft_type,
                location,
                operator,
                summary AS narrative
            FROM public.pci_scraped_accidents
            WHERE uid = :uid
            """
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported UID prefix '{prefix}'. Expected one of: 'asn_', 'asrs_', 'pci_'.",
        )

    result = await db.execute(query, {"uid": uid})
    row = result.mappings().first()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Record not found for the provided UID.",
        )

    # Normalize the response into a standard JSON structure
    return {
        "uid": row["uid"],
        "date": row.get("date"),
        "phase": row.get("phase"),
        "aircraft_type": row.get("aircraft_type"),
        "location": row.get("location"),
        "operator": row.get("operator"),
        "narrative": row.get("narrative"),
    }


@app.get("/classification-results")
async def get_classification_results(db: AsyncSession = Depends(get_db)) -> List[Dict[str, Any]]:
    """
    Fetches all classification results from public.classification_results.
    Returns every column requested in a list of dictionaries.
    """
    query = text(
        """
        SELECT
            id,
            source_uid,
            bert_results,
            llm1_category,
            llm1_confidence,
            llm1_reasoning,
            llm2_category,
            llm2_confidence,
            llm2_reasoning,
            llm3_category,
            llm3_confidence,
            llm3_reasoning,
            final_category,
            final_confidence,
            routing_decision,
            consensus_rule,
            rule_explanation,
            processing_time_ms,
            processed_at
        FROM public.classification_results
        """
    )

    result = await db.execute(query)
    rows = result.mappings().all()

    return [dict(row) for row in rows]


@app.get("/full_classification_results/{uid}")
async def get_full_classification_results(
    uid: str, db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """
    Returns a single classification result joined with its originating report table,
    determined by the prefix portion of the UID.
    """
    prefix = uid.split("_", 1)[0]

    if prefix == "asn":
        query = text(
            """
            SELECT
                cr.id,
                cr.source_uid,
                cr.bert_results,
                cr.llm1_category,
                cr.llm1_confidence,
                cr.llm1_reasoning,
                cr.llm2_category,
                cr.llm2_confidence,
                cr.llm2_reasoning,
                cr.llm3_category,
                cr.llm3_confidence,
                cr.llm3_reasoning,
                cr.final_category,
                cr.final_confidence,
                cr.routing_decision,
                cr.consensus_rule,
                cr.rule_explanation,
                cr.processing_time_ms,
                cr.processed_at,
                origin.uid AS origin_uid,
                origin.date AS origin_date,
                origin.phase AS origin_phase,
                origin.aircraft_type AS origin_aircraft_type,
                origin.location AS origin_location,
                origin.operator AS origin_operator,
                origin.narrative AS origin_narrative
            FROM public.classification_results cr
            JOIN public.asn_scraped_accidents origin ON origin.uid = cr.source_uid
            WHERE cr.source_uid = :uid
            """
        )
    elif prefix == "asrs":
        query = text(
            """
            SELECT
                cr.id,
                cr.source_uid,
                cr.bert_results,
                cr.llm1_category,
                cr.llm1_confidence,
                cr.llm1_reasoning,
                cr.llm2_category,
                cr.llm2_confidence,
                cr.llm2_reasoning,
                cr.llm3_category,
                cr.llm3_confidence,
                cr.llm3_reasoning,
                cr.final_category,
                cr.final_confidence,
                cr.routing_decision,
                cr.consensus_rule,
                cr.rule_explanation,
                cr.processing_time_ms,
                cr.processed_at,
                origin.uid AS origin_uid,
                origin.time AS origin_date,
                origin.phase AS origin_phase,
                origin.aircraft_type AS origin_aircraft_type,
                origin.place AS origin_location,
                origin.operator AS origin_operator,
                origin.synopsis AS origin_narrative
            FROM public.classification_results cr
            JOIN public.asrs_records origin ON origin.uid = cr.source_uid
            WHERE cr.source_uid = :uid
            """
        )
    elif prefix == "pci":
        query = text(
            """
            SELECT
                cr.id,
                cr.source_uid,
                cr.bert_results,
                cr.llm1_category,
                cr.llm1_confidence,
                cr.llm1_reasoning,
                cr.llm2_category,
                cr.llm2_confidence,
                cr.llm2_reasoning,
                cr.llm3_category,
                cr.llm3_confidence,
                cr.llm3_reasoning,
                cr.final_category,
                cr.final_confidence,
                cr.routing_decision,
                cr.consensus_rule,
                cr.rule_explanation,
                cr.processing_time_ms,
                cr.processed_at,
                origin.uid AS origin_uid,
                origin.date AS origin_date,
                NULL AS origin_phase,
                origin.ac_________type AS origin_aircraft_type,
                origin.location AS origin_location,
                origin.operator AS origin_operator,
                origin.summary AS origin_narrative
            FROM public.classification_results cr
            JOIN public.pci_scraped_accidents origin ON origin.uid = cr.source_uid
            WHERE cr.source_uid = :uid
            """
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported UID prefix '{prefix}'. Expected one of: 'asn_', 'asrs_', 'pci_'.",
        )

    result = await db.execute(query, {"uid": uid})
    row = result.mappings().first()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No classification result found for the provided UID.",
        )

    classification_data = {
        "id": row["id"],
        "source_uid": row["source_uid"],
        "bert_results": row["bert_results"],
        "llm1_category": row["llm1_category"],
        "llm1_confidence": row["llm1_confidence"],
        "llm1_reasoning": row["llm1_reasoning"],
        "llm2_category": row["llm2_category"],
        "llm2_confidence": row["llm2_confidence"],
        "llm2_reasoning": row["llm2_reasoning"],
        "llm3_category": row["llm3_category"],
        "llm3_confidence": row["llm3_confidence"],
        "llm3_reasoning": row["llm3_reasoning"],
        "final_category": row["final_category"],
        "final_confidence": row["final_confidence"],
        "routing_decision": row["routing_decision"],
        "consensus_rule": row["consensus_rule"],
        "rule_explanation": row["rule_explanation"],
        "processing_time_ms": row["processing_time_ms"],
        "processed_at": row["processed_at"],
    }

    origin_data = {
        "uid": row["origin_uid"],
        "date": row.get("origin_date"),
        "phase": row.get("origin_phase"),
        "aircraft_type": row.get("origin_aircraft_type"),
        "location": row.get("origin_location"),
        "operator": row.get("origin_operator"),
        "narrative": row.get("origin_narrative"),
    }

    return {"classification": classification_data, "origin": origin_data}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


