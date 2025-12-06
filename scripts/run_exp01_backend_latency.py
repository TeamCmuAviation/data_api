import time, requests, json
from utils import ensure_dir, write_csv, save_plot, log
import numpy as np

API = "http://127.0.0.1:58510/full_classification_results_bulk"
UIDFILE = "uids_for_testing.json"       # create a JSON file with 10k UIDs

SIZES = [10, 100, 1000, 10000]
REPEATS = 5
OUTDIR = "../results/EXP01/"
ensure_dir(OUTDIR)

# Load UIDs
uids = json.load(open(UIDFILE))

rows = []

log("Starting EXP01 Backend Latency Test")

for size in SIZES:
    batch = uids[:size]
    for r in range(REPEATS):
        t0 = time.time()
        response = requests.post(API, json=batch)
        dt = time.time() - t0
        rows.append([size, r+1, dt, response.status_code, len(response.content)])
        log(f"Batch {size}, Run {r+1}, Time {dt}")

write_csv(OUTDIR + "raw_latency.csv",
          ["batch_size", "run", "latency_sec", "status", "response_bytes"],
          rows)

# Summary plot
sizes = sorted(set(r[0] for r in rows))
means = [np.mean([x[2] for x in rows if x[0] == s]) for s in sizes]
save_plot(sizes, means,
          "Batch Size", "Average Latency (s)",
          "EXP01 Backend Latency", OUTDIR + "latency_plot.png")

log("EXP01 completed.")
