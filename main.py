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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


