import psycopg2
import json

# -----------------------------------
# CONFIGURATION
# -----------------------------------

DB_CONFIG = {
    "host": "172.29.98.161",
    "database": "aviation_db",
    "user": "manyara",
    "password": "toormaster"
}

OUTPUT_FILE = "uids_for_testing.json"

# Number of UIDs you want to save
# You can change this to 1000, 10000, etc.
NUM_UIDS = 10000

# Random or deterministic?
RANDOM_SAMPLING = True   # set to False for ordering by UID


# -----------------------------------
# CONNECT TO DATABASE
# -----------------------------------

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

print("Connected to database.")

# -----------------------------------
# QUERY UIDS
# -----------------------------------

if RANDOM_SAMPLING:
    sql = f"""
        SELECT source_uid
        FROM classification_results
        ORDER BY RANDOM();
    """
else:
    sql = f"""
        SELECT         SELECT source_uid

        FROM classification_results
        ORDER BY uid;
    """

cur.execute(sql)

uids = [row[0] for row in cur.fetchall()]

print(f"Retrieved {len(uids)} UIDs from database.")

# -----------------------------------
# SAVE TO JSON FILE
# -----------------------------------

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(uids, f, indent=2)

print(f"Saved UIDs to {OUTPUT_FILE}")

# -----------------------------------
# CLEANUP
# -----------------------------------

cur.close()
conn.close()

print("Done.")
