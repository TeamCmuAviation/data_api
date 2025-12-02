import time
from utils import ensure_dir, write_csv, save_plot, log
import requests

OUTDIR = "../results/EXP03/"
ensure_dir(OUTDIR)

URL = "http://localhost:8501"  # default Streamlit port
REPEATS = 10

rows = []
log("Starting EXP03 Streamlit Load-Time Test")

for r in range(REPEATS):
    t0 = time.time()
    try:
        html = requests.get(URL, timeout=30)
        dt = time.time() - t0
        rows.append([r+1, dt, html.status_code])
        log(f"Run {r+1}: Load time={dt}s")
    except:
        rows.append([r+1, None, "TIMEOUT"])

write_csv(OUTDIR + "ui_load_times.csv",
          ["run", "load_time_sec", "status"],
          rows)

save_plot([r[0] for r in rows],
          [r[1] if r[1] else 0 for r in rows],
          "Run", "Load Time (s)",
          "EXP03 UI Load Time", OUTDIR + "ui_load_plot.png")

log("EXP03 Completed.")
