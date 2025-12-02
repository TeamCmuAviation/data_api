import time, json, requests
from utils import ensure_dir, write_csv, save_plot, log

OUTDIR = "../results/EXP04/"
ensure_dir(OUTDIR)

API = "http://127.0.0.1:8000/full_classification_results_bulk"
TEST_FILTERS = [
    ("operator", "Kenya Airways"),
    ("aircraft_type", "Boeing 737"),
    ("phase_of_flight", "LANDING")
]

uids = json.load(open("uids_for_testing.json"))[:500]

rows = []
log("Starting EXP04 Filter Latency Test")

for fname, fval in TEST_FILTERS:
    for r in range(5):
        t0 = time.time()
        response = requests.post(API, json=uids).json()
        # Apply filter simulation
        filtered = [x for x in response if x.get(fname) == fval]
        dt = time.time() - t0

        rows.append([fname, fval, r+1, dt, len(filtered)])
        log(f"{fname}={fval}, run={r+1}, latency={dt}")

write_csv(OUTDIR + "filter_latency.csv",
          ["filter", "value", "run", "latency_sec", "matches"],
          rows)

log("EXP04 Completed.")
