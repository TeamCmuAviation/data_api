from typing import Any, Dict, List
from fastapi import FastAPI, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db
import pandas as pd

app = FastAPI()


@app.get("/airports")
async def get_airports(codes: List[str], db: AsyncSession = Depends(get_db)):
    if not codes:
        return {}
    codes_lower = [c.lower() for c in codes]
    query = text("""
        SELECT icao_code, iata_code, name, city, country, lat, lon
        FROM airport_location
        WHERE LOWER(icao_code) = ANY(:codes)
    """)
    result = await db.execute(query, {"codes": codes_lower})
    return {row["icao_code"]: dict(row) for row in result.mappings().all()}


@app.get("/classification-results")
async def get_classification_results(db: AsyncSession = Depends(get_db)) -> List[Dict[str, Any]]:
    query = text("SELECT * FROM public.classification_results")
    result = await db.execute(query)
    return [dict(row) for row in result.mappings().all()]


@app.post("/full_classification_results_bulk")
async def get_full_classification_results_bulk(uids: List[str], db: AsyncSession = Depends(get_db)):
    if not uids:
        return {"results": {}, "aggregates": {}}

    asn_uids = [uid for uid in uids if uid.startswith("asn_")]
    asrs_uids = [uid for uid in uids if uid.startswith("asrs_")]
    pci_uids = [uid for uid in uids if uid.startswith("pci_")]

    results = []

    async def fetch_rows(source_uids, origin_table, columns_map):
        if not source_uids:
            return []
        query = text(f"""
            SELECT cr.*, {columns_map}
            FROM public.classification_results cr
            JOIN public.{origin_table} origin ON origin.uid = cr.source_uid
            WHERE cr.source_uid = ANY(:uids)
        """)
        res = await db.execute(query, {"uids": source_uids})
        return [dict(row) for row in res.mappings().all()]

    results += await fetch_rows(asn_uids, "asn_scraped_accidents",
        "origin.uid AS origin_uid, origin.date AS origin_date, origin.phase AS origin_phase, origin.aircraft_type AS origin_aircraft_type, origin.location AS origin_location, origin.operator AS origin_operator, origin.narrative AS origin_narrative"
    )

    results += await fetch_rows(asrs_uids, "asrs_records",
        "origin.uid AS origin_uid, origin.time AS origin_date, origin.phase AS origin_phase, origin.aircraft_type AS origin_aircraft_type, origin.place AS origin_location, origin.operator AS origin_operator, origin.synopsis AS origin_narrative"
    )

    results += await fetch_rows(pci_uids, "pci_scraped_accidents",
        "origin.uid AS origin_uid, origin.date AS origin_date, NULL AS origin_phase, origin.aircraft_type AS origin_aircraft_type, origin.location AS origin_location, origin.operator AS origin_operator, origin.summary AS origin_narrative"
    )

    df = pd.DataFrame(results)
    aggregates = {}
    if not df.empty:
        aggregates = {
            "total_incidents": len(df),
            "unique_operators": df.get("origin_operator", pd.Series()).nunique(),
            "unique_aircraft_types": df.get("origin_aircraft_type", pd.Series()).nunique(),
            "phase_counts": df.get("origin_phase", pd.Series()).value_counts().to_dict(),
            "operator_counts": df.get("origin_operator", pd.Series()).value_counts().to_dict()
        }

    return {"results": {row["source_uid"]: row for row in results}, "aggregates": aggregates}
