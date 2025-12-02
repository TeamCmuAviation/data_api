import os
import csv
import json
import time
import requests
import statistics
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for r in rows:
            writer.writerow(r)

def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def save_plot(x, y, xlabel, ylabel, title, outfile):
    plt.figure(figsize=(8,5))
    plt.plot(x, y)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(outfile)
    plt.close()

def log(msg):
    print(f"[{timestamp()}] {msg}")
