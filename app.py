import streamlit as st
import pandas as pd
import requests
from typing import List, Dict

# -----------------------------
# Config
# -----------------------------
API_URL = "http://127.0.0.1:58510"  # FastAPI server

st.set_page_config(page_title="Aviation Dashboard", layout="wide")

# -----------------------------
# Fetch full classification results (bulk)
# -----------------------------
@st.cache_data(show_spinner=True)
def load_classification_data(uids: List[str]):
    response = requests.post(f"{API_URL}/full_classification_results_bulk", json=uids)
    if response.status_code != 200:
        st.error("Failed to load data from API")
        return pd.DataFrame(), {}
    data = response.json()
    df = pd.DataFrame(list(data["results"].values()))
    aggregates = data.get("aggregates", {})
    return df, aggregates


# -----------------------------
# Load UIDs (example: all from classification results)
# -----------------------------
@st.cache_data(show_spinner=True)
def load_all_uids() -> List[str]:
    """Fetches all UIDs by making paginated requests to the API."""
    all_uids = []
    skip = 0
    limit = 500  # Fetch in chunks of 500
    while True:
        response = requests.get(
            f"{API_URL}/classification-results", params={"skip": skip, "limit": limit}
        )
        if response.status_code != 200:
            st.error(f"Failed to fetch UIDs on page starting at {skip}")
            return []
        data = response.json()
        if not data:
            break  # No more data to fetch
        all_uids.extend([row["source_uid"] for row in data])
        skip += limit
    return all_uids


# -----------------------------
# Main
# -----------------------------
st.title("Aviation Safety Dashboard")

uids = load_all_uids()
df, aggregates = load_classification_data(uids)

# -----------------------------
# Filters
# -----------------------------
st.sidebar.header("Filters")
operators = st.sidebar.multiselect("Operator", options=df["origin_operator"].dropna().unique())
phases = st.sidebar.multiselect("Flight Phase", options=df["origin_phase"].dropna().unique())
aircraft_types = st.sidebar.multiselect("Aircraft Type", options=df["origin_aircraft_type"].dropna().unique())

filtered_df = df.copy()
if operators:
    filtered_df = filtered_df[filtered_df["origin_operator"].isin(operators)]
if phases:
    filtered_df = filtered_df[filtered_df["origin_phase"].isin(phases)]
if aircraft_types:
    filtered_df = filtered_df[filtered_df["origin_aircraft_type"].isin(aircraft_types)]

# -----------------------------
# KPIs
# -----------------------------
st.subheader("Key Metrics")
cols = st.columns(4)
cols[0].metric("Total Incidents", aggregates.get("total_incidents", 0))
cols[1].metric("Unique Operators", aggregates.get("unique_operators", 0))
cols[2].metric("Unique Aircraft Types", aggregates.get("unique_aircraft_types", 0))
cols[3].metric("Filtered Incidents", len(filtered_df))

# -----------------------------
# Charts
# -----------------------------
st.subheader("Incidents by Flight Phase")
phase_counts = filtered_df["origin_phase"].value_counts()
st.bar_chart(phase_counts)

st.subheader("Incidents by Operator")
operator_counts = filtered_df["origin_operator"].value_counts()
st.bar_chart(operator_counts)

# -----------------------------
# Table
# -----------------------------
st.subheader("Incident Records")
st.dataframe(filtered_df)
