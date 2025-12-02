import time, json, requests
from utils import ensure_dir, write_csv, save_plot, log

OUTDIR = "../results/EXP05/"
ensure_dir(OUTDIR)

API = "http://127.0.0.1:8000/full_classification_results_bulk"
uids = json.load(open("uids_for_testing.json"))[:1000]

rows = []
log("Starting EXP05 Cache Effectiveness Test")

for rep in range(8):
    t0 = time.time()
    r = requests.post(API, json=uids)
    dt = time.time() - t0
    rows.append([rep+1, dt, r.status_code])
    log(f"Run {rep+1}, Latency={dt}")

write_csv(OUTDIR + "cache_tests.csv",
          ["run", "latency_sec", "status"],
          rows)

log("EXP05 Completed.")
