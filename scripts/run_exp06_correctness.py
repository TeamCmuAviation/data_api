import json, pandas as pd, requests
from utils import ensure_dir, write_csv, log

OUTDIR = "../results/EXP06/"
ensure_dir(OUTDIR)

API = "http://127.0.0.1:8000/full_classification_results_bulk"
TEST_UIDS = json.load(open("uids_for_testing.json"))[:100]

log("Starting EXP06 Correctness Test")

# Query API
api_res = requests.post(API, json=TEST_UIDS).json()
api_df = pd.DataFrame(api_res)

# Query DB directly (manual SQL)
import psycopg2
conn = psycopg2.connect(host="172.29.98.161", dbname="aviation_db",
                        user="manyara", password="toormaster")

sql = """
SELECT * FROM classification_results
WHERE source_uid = ANY(%s);
"""

cur = conn.cursor()
cur.execute(sql, (TEST_UIDS,))
db_rows = cur.fetchall()
cols = [desc[0] for desc in cur.description]
db_df = pd.DataFrame(db_rows, columns=cols)

api_df.head().to_csv(OUTDIR + "api_sample.csv", index=False)
db_df.head().to_csv(OUTDIR + "db_sample.csv", index=False)
# Compare
merged = api_df.merge(db_df, on="source_uid", how="inner", suffixes=("_api", "_db"))
merged.to_csv(OUTDIR + "correctness_join.csv", index=False)

log("EXP06 Completed.")
