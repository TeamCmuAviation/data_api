import concurrent.futures
import time, requests, json
from utils import ensure_dir, write_csv, save_plot, log

API = "http://127.0.0.1:8000/full_classification_results_bulk"
UIDFILE = "uids_for_testing.json"
CONCURRENCY = [10, 50, 100]
REPEATS = 3
OUTDIR = "../results/EXP02/"

ensure_dir(OUTDIR)
uids = json.load(open(UIDFILE))[:100]  # small batch for concurrency test

def send_request():
    t0 = time.time()
    r = requests.post(API, json=uids)
    return time.time() - t0, r.status_code

rows = []
log("Starting EXP02 Concurrency Test")

for c in CONCURRENCY:
    for rep in range(REPEATS):
        with concurrent.futures.ThreadPoolExecutor(max_workers=c) as ex:
            futures = [ex.submit(send_request) for _ in range(c)]
            results = [f.result() for f in futures]
        
        latencies = [r[0] for r in results]
        statuses = [r[1] for r in results]
        rows.append([c, rep+1, sum(latencies)/len(latencies), max(latencies), statuses.count(200)])

        log(f"Concurrency={c}, Rep={rep+1}, Mean={sum(latencies)/len(latencies)}")

write_csv(OUTDIR + "raw_concurrency.csv",
          ["concurrency", "rep", "mean_latency", "max_latency", "ok_count"],
          rows)

save_plot([r[0] for r in rows], [r[2] for r in rows],
          "Concurrency", "Mean Latency (s)",
          "EXP02 Concurrency Performance", OUTDIR + "concurrency_plot.png")

log("EXP02 Completed.")
