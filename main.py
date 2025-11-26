from typing import Any, Dict

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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


