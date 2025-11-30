"""
Aviation Safety Incidents Dashboard
A Streamlit application for visualizing and analyzing aviation safety incident data.
"""

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from datetime import datetime
from typing import Any, Dict, List, Optional

from typing import Optional, Dict, Any
import httpx  # async-capable HTTP client
import asyncio

# ============================================================================
# Configuration
# ============================================================================

BASE_API_URL = "http://localhost:8000"  # Update this to your FastAPI server URL
LOGO_PATH = "config/logo.jpg"  # Path to logo image (e.g., "config/logo.png") or empty string

# CMU Tartan-inspired color palette
COLORS = {
    "low": "#1E3A8A",      # Deep blue
    "medium": "#F59E0B",   # Amber
    "high": "#DC2626",     # Deep red
    "accent_blue": "#3B82F6",
    "accent_red": "#B91C1C",
    "background": "#F8FAFC",
    "text": "#1E293B",
}

# Severity mapping (can be customized based on category or confidence)
SEVERITY_MAPPING = {
    "low": ["minor", "low", "routine"],
    "medium": ["medium", "moderate", "warning"],
    "high": ["high", "severe", "critical", "accident", "incident"],
}


# ============================================================================
# Geocoding Stub
# ============================================================================

def get_airport_coordinates(code: str) -> Optional[Dict[str, Any]]:
    """
    Given an ICAO or IATA airport code string, fetch airport metadata from the FastAPI endpoint.
    Returns:
    {
        "lat": float,
        "lon": float,
        "city": str | None,
        "country": str | None,
        "name": str | None
    }
    or None if unknown.
    """
    url = f"{BASE_API_URL}/airport/{code}"

    try:
        # Using synchronous HTTP call for Streamlit
        response = httpx.get(url, timeout=5.0)
        if response.status_code == 200:
            data = response.json()
            return {
                "lat": data.get("lat"),
                "lon": data.get("lon"),
                "city": data.get("city"),
                "country": data.get("country"),
                "name": data.get("name"),
            }
        else:
            return None
    except Exception as e:
        st.error(f"Error fetching airport data: {e}")
        return None
    
# ============================================================================
# Data Fetching Functions
# ============================================================================

@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_classification_results(base_url: str) -> pd.DataFrame:
    """
    Fetch all classification results from the FastAPI endpoint.
    Returns a DataFrame with classification data.
    """
    try:
        url = f"{base_url}/classification-results"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        return df
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching classification results: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=600)  # Cache for 10 minutes
def fetch_origin_data(uid: str, base_url: str) -> Dict[str, Any]:
    """
    Fetch origin data for a single UID. Cached to avoid redundant API calls.
    """
    try:
        url = f"{base_url}/full_classification_results/{uid}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        origin = data.get("origin", {})
        return {
            "uid": origin.get("uid"),
            "date": origin.get("date"),
            "phase": origin.get("phase"),
            "aircraft_type": origin.get("aircraft_type"),
            "location": origin.get("location"),
            "operator": origin.get("operator"),
            "narrative": origin.get("narrative"),
        }
    except requests.exceptions.RequestException:
        # If fetch fails, return None values
        return {
            "uid": uid,
            "date": None,
            "phase": None,
            "aircraft_type": None,
            "location": None,
            "operator": None,
            "narrative": None,
        }


def enrich_with_origin(df: pd.DataFrame, base_url: str) -> pd.DataFrame:
    """
    Enrich classification DataFrame with origin data from /full_classification_results/{uid}.
    Uses cached fetch function to avoid redundant API calls.
    """
    if df.empty or "source_uid" not in df.columns:
        return df
    
    # Get unique UIDs
    unique_uids = df["source_uid"].dropna().unique().tolist()
    
    if not unique_uids:
        return df
    
    # Fetch origin data for each unique UID (with caching via fetch_origin_data)
    origin_data = {}
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for idx, uid in enumerate(unique_uids):
        origin_data[uid] = fetch_origin_data(uid, base_url)
        progress_bar.progress((idx + 1) / len(unique_uids))
        status_text.text(f"Enriching data: {idx + 1}/{len(unique_uids)}")
    
    progress_bar.empty()
    status_text.empty()
    
    # Merge origin data into main DataFrame
    origin_df = pd.DataFrame.from_dict(origin_data, orient="index")
    origin_df.index = origin_df["uid"]
    
    # Merge on source_uid
    enriched_df = df.merge(
        origin_df[["date", "phase", "aircraft_type", "location", "operator", "narrative"]],
        left_on="source_uid",
        right_index=True,
        how="left",
        suffixes=("", "_origin")
    )
    
    return enriched_df


def prepare_canonical_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare a canonical DataFrame with all required fields for visualization.
    """
    if df.empty:
        return df
    
    canonical = df.copy()
    
    # Rename source_uid to uid for consistency
    if "source_uid" in canonical.columns:
        canonical["uid"] = canonical["source_uid"]
    
    # Convert date to datetime
    if "date" in canonical.columns:
        canonical["date"] = pd.to_datetime(canonical["date"], errors="coerce")
        canonical["year"] = canonical["date"].dt.year
        canonical["month"] = canonical["date"].dt.month
        canonical["period"] = canonical["date"].dt.to_period("M")
    
    # Set location_code
    if "location" in canonical.columns:
        canonical["location_code"] = canonical["location"]
    
    # Derive severity from final_category or final_confidence
    if "final_category" in canonical.columns:
        canonical["severity"] = canonical["final_category"].apply(
            lambda x: derive_severity(x, None)
        )
    elif "final_confidence" in canonical.columns:
        # Use confidence as proxy: high confidence might indicate high severity
        canonical["severity"] = canonical["final_confidence"].apply(
            lambda x: "high" if x and x > 0.8 else ("medium" if x and x > 0.5 else "low")
        )
    else:
        canonical["severity"] = "medium"  # Default
    
    # Add geocoding data (will be populated when needed for map)
    # We'll handle this in prepare_map_data to avoid unnecessary lookups
    
    return canonical


def derive_severity(category: str, confidence: Optional[float]) -> str:
    """
    Derive severity level from category name or confidence.
    """
    if not category:
        return "medium"
    
    category_lower = str(category).lower()
    
    for severity, keywords in SEVERITY_MAPPING.items():
        if any(keyword in category_lower for keyword in keywords):
            return severity
    
    return "medium"  # Default


# ============================================================================
# Filter Functions
# ============================================================================

def render_sidebar_filters(df: pd.DataFrame) -> Dict:
    """
    Render sidebar filters and return a dictionary of filter values.
    """
    filters = {}
    
    st.sidebar.header("Filters")
    
    # Date range filter
    st.sidebar.subheader("Date Range")
    
    if not df.empty and "date" in df.columns and df["date"].notna().any():
        min_date = df["date"].min()
        max_date = df["date"].max()
        
        if pd.notna(min_date) and pd.notna(max_date):
            # Extract year and month for selection
            min_year = int(min_date.year)
            min_month = int(min_date.month)
            max_year = int(max_date.year)
            max_month = int(max_date.month)
            
            # Year selection
            years = list(range(min_year, max_year + 1))
            if years:
                start_year = st.sidebar.selectbox("Start Year", years, index=0)
                end_year = st.sidebar.selectbox("End Year", years, index=len(years) - 1)
                
                # Month selection
                months = list(range(1, 13))
                start_month = st.sidebar.selectbox("Start Month", months, index=min_month - 1 if start_year == min_year else 0)
                end_month = st.sidebar.selectbox("End Month", months, index=max_month - 1 if end_year == max_year else 11)
                
                filters["start_year"] = start_year
                filters["start_month"] = start_month
                filters["end_year"] = end_year
                filters["end_month"] = end_month
    else:
        st.sidebar.info("No date data available")
        filters["start_year"] = None
        filters["start_month"] = None
        filters["end_year"] = None
        filters["end_month"] = None
    
    # Location filter
    if not df.empty and "location_code" in df.columns:
        locations = sorted(df["location_code"].dropna().unique().tolist())
        if locations:
            selected_locations = st.sidebar.multiselect(
                "Airport Code",
                locations,
                default=[]
            )
            filters["locations"] = selected_locations
        else:
            filters["locations"] = []
    else:
        filters["locations"] = []
    
    # Phase filter
    if not df.empty and "phase" in df.columns:
        phases = sorted(df["phase"].dropna().unique().tolist())
        if phases:
            selected_phases = st.sidebar.multiselect(
                "Phase of Flight",
                phases,
                default=[]
            )
            filters["phases"] = selected_phases
        else:
            filters["phases"] = []
    else:
        filters["phases"] = []
    
    # Operator filter
    if not df.empty and "operator" in df.columns:
        operators = sorted(df["operator"].dropna().unique().tolist())
        if operators:
            selected_operators = st.sidebar.multiselect(
                "Operator",
                operators,
                default=[]
            )
            filters["operators"] = selected_operators
        else:
            filters["operators"] = []
    else:
        filters["operators"] = []
    
    # Final category filter
    if not df.empty and "final_category" in df.columns:
        categories = sorted(df["final_category"].dropna().unique().tolist())
        if categories:
            selected_categories = st.sidebar.multiselect(
                "Final Category",
                categories,
                default=[]
            )
            filters["categories"] = selected_categories
        else:
            filters["categories"] = []
    else:
        filters["categories"] = []
    
    # Severity filter
    if not df.empty and "severity" in df.columns:
        severities = sorted(df["severity"].dropna().unique().tolist())
        if severities:
            selected_severities = st.sidebar.multiselect(
                "Severity",
                severities,
                default=[]
            )
            filters["severities"] = selected_severities
        else:
            filters["severities"] = []
    else:
        filters["severities"] = []
    
    return filters


def apply_filters(df: pd.DataFrame, filters: Dict) -> pd.DataFrame:
    """
    Apply filters to the DataFrame and return filtered results.
    """
    if df.empty:
        return df
    
    filtered_df = df.copy()
    
    # Date range filter
    if filters.get("start_year") and filters.get("start_month"):
        start_date = pd.Timestamp(year=filters["start_year"], month=filters["start_month"], day=1)
        if filters.get("end_year") and filters.get("end_month"):
            # Get last day of end month
            if filters["end_month"] == 12:
                end_date = pd.Timestamp(year=filters["end_year"] + 1, month=1, day=1) - pd.Timedelta(days=1)
            else:
                end_date = pd.Timestamp(year=filters["end_year"], month=filters["end_month"] + 1, day=1) - pd.Timedelta(days=1)
        else:
            end_date = filtered_df["date"].max() if "date" in filtered_df.columns else None
        
        if "date" in filtered_df.columns and pd.notna(start_date) and pd.notna(end_date):
            filtered_df = filtered_df[
                (filtered_df["date"] >= start_date) & (filtered_df["date"] <= end_date)
            ]
    
    # Location filter
    if filters.get("locations"):
        if "location_code" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["location_code"].isin(filters["locations"])]
    
    # Phase filter
    if filters.get("phases"):
        if "phase" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["phase"].isin(filters["phases"])]
    
    # Operator filter
    if filters.get("operators"):
        if "operator" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["operator"].isin(filters["operators"])]
    
    # Category filter
    if filters.get("categories"):
        if "final_category" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["final_category"].isin(filters["categories"])]
    
    # Severity filter
    if filters.get("severities"):
        if "severity" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["severity"].isin(filters["severities"])]
    
    return filtered_df


# ============================================================================
# Data Preparation Functions
# ============================================================================

def prepare_time_series(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare time series data grouped by month.
    """
    if df.empty or "date" not in df.columns:
        return pd.DataFrame()
    
    # Filter out rows without valid dates
    ts_df = df[df["date"].notna()].copy()
    
    if ts_df.empty:
        return pd.DataFrame()
    
    # Create period column
    ts_df["period"] = ts_df["date"].dt.to_period("M")
    
    # Group by period and count
    time_series = ts_df.groupby("period").size().reset_index(name="incident_count")
    time_series["period_str"] = time_series["period"].astype(str)
    
    return time_series


def prepare_map_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare aggregated data for map visualization.
    """
    if df.empty or "location_code" not in df.columns:
        return pd.DataFrame()
    
    # Filter out rows without location codes
    map_df = df[df["location_code"].notna()].copy()
    
    if map_df.empty:
        return pd.DataFrame()
    
    # Group by location_code
    agg_data = map_df.groupby("location_code").agg({
        "uid": "count",
        "severity": lambda x: (x == "high").sum() if "severity" in map_df.columns else 0,
    }).reset_index()
    
    agg_data.columns = ["location_code", "incident_count", "high_severity_count"]
    
    # Add geocoding data
    coordinates_data = []
    for code in agg_data["location_code"]:
        coords = get_airport_coordinates(code)
        if coords:
            coordinates_data.append({
                "lat": coords.get("lat"),
                "lon": coords.get("lon"),
                "city": coords.get("city"),
                "country": coords.get("country"),
                "airport_name": coords.get("name"),
            })
        else:
            coordinates_data.append({
                "lat": None,
                "lon": None,
                "city": None,
                "country": None,
                "airport_name": None,
            })
    
    coords_df = pd.DataFrame(coordinates_data)
    map_data = pd.concat([agg_data, coords_df], axis=1)
    
    # Filter out rows without coordinates
    map_data = map_data[map_data["lat"].notna() & map_data["lon"].notna()]
    
    return map_data


# ============================================================================
# Rendering Functions
# ============================================================================

def render_header():
    """
    Render the header section with logo and title.
    """
    cols = st.columns([1, 4])
    
    with cols[0]:
        if LOGO_PATH:
            try:
                st.image(LOGO_PATH, use_container_width=True)
            except Exception:
                pass  # Silently fail if logo not found
    
    with cols[1]:
        st.title("Aviation Safety Incidents Dashboard")
        st.caption("Single-look situational awareness for safety managers")


def render_kpis(df: pd.DataFrame):
    """
    Render KPI cards at the top of the dashboard.
    """
    if df.empty:
        st.warning("No data available for KPI calculation")
        return
    
    kpi_cols = st.columns(4)
    
    # Total incidents
    total_incidents = len(df)
    with kpi_cols[0]:
        st.metric("Total Incidents", f"{total_incidents:,}")
    
    # This month vs previous month
    if "date" in df.columns and df["date"].notna().any():
        current_date = datetime.now()
        current_month = df[
            (df["date"].dt.year == current_date.year) &
            (df["date"].dt.month == current_date.month)
        ]
        prev_month = current_date.month - 1 if current_date.month > 1 else 12
        prev_year = current_date.year if current_date.month > 1 else current_date.year - 1
        previous_month = df[
            (df["date"].dt.year == prev_year) &
            (df["date"].dt.month == prev_month)
        ]
        
        current_count = len(current_month)
        prev_count = len(previous_month)
        delta = current_count - prev_count
        
        with kpi_cols[1]:
            st.metric(
                "This Month",
                f"{current_count:,}",
                delta=f"{delta:+,}" if prev_count > 0 else None
            )
    else:
        with kpi_cols[1]:
            st.metric("This Month", "N/A")
    
    # Unique airports
    if "location_code" in df.columns:
        unique_airports = df["location_code"].nunique()
        with kpi_cols[2]:
            st.metric("Unique Airports", f"{unique_airports:,}")
    else:
        with kpi_cols[2]:
            st.metric("Unique Airports", "N/A")
    
    # High severity incidents
    if "severity" in df.columns:
        high_severity = len(df[df["severity"] == "high"])
        with kpi_cols[3]:
            st.metric("High Severity", f"{high_severity:,}")
    else:
        with kpi_cols[3]:
            st.metric("High Severity", "N/A")


def render_time_series(df: pd.DataFrame):
    """
    Render time series chart of incidents over time.
    """
    st.subheader("Incidents Over Time")
    
    time_series_data = prepare_time_series(df)
    
    if time_series_data.empty:
        st.info("No time series data available")
        return
    
    # Create line chart
    fig = px.line(
        time_series_data,
        x="period_str",
        y="incident_count",
        title="Monthly Incident Count",
        labels={
            "period_str": "Month",
            "incident_count": "Number of Incidents"
        },
        markers=True,
    )
    
    fig.update_layout(
        xaxis_title="Month",
        yaxis_title="Number of Incidents",
        hovermode="x unified",
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    
    fig.update_traces(
        line_color=COLORS["accent_blue"],
        marker_color=COLORS["accent_blue"],
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_map(df: pd.DataFrame):
    """
    Render map visualization of incident hotspots.
    """
    st.subheader("Geographic Hotspots")
    
    map_data = prepare_map_data(df)
    
    if map_data.empty:
        st.info("No geographic data available (airport coordinates not found)")
        return
    
    # Create scatter mapbox
    fig = px.scatter_mapbox(
        map_data,
        lat="lat",
        lon="lon",
        size="incident_count",
        color="incident_count",
        hover_name="location_code",
        hover_data={
            "airport_name": True,
            "city": True,
            "country": True,
            "incident_count": True,
            "high_severity_count": True,
            "lat": False,
            "lon": False,
        },
        color_continuous_scale="Reds",
        size_max=30,
        zoom=2,
        height=600,
        title="Incident Distribution by Airport",
    )
    
    fig.update_layout(
        mapbox_style="carto-positron",
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    
    st.plotly_chart(fig, use_container_width=True)


def render_bar_charts(df: pd.DataFrame):
    """
    Render supporting bar charts in columns.
    """
    if df.empty:
        return
    
    chart_cols = st.columns(3)
    
    # Severity bar chart
    with chart_cols[0]:
        st.subheader("Incidents by Severity")
        if "severity" in df.columns:
            severity_counts = df["severity"].value_counts().reset_index()
            severity_counts.columns = ["severity", "count"]
            
            # Map colors
            severity_counts["color"] = severity_counts["severity"].map(
                lambda x: COLORS.get(x, COLORS["medium"])
            )
            
            fig = px.bar(
                severity_counts,
                x="severity",
                y="count",
                color="severity",
                color_discrete_map={
                    "low": COLORS["low"],
                    "medium": COLORS["medium"],
                    "high": COLORS["high"],
                },
                labels={"severity": "Severity", "count": "Count"},
            )
            fig.update_layout(showlegend=False, plot_bgcolor="white", paper_bgcolor="white")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No severity data available")
    
    # Category bar chart (top N)
    with chart_cols[1]:
        st.subheader("Top Categories")
        if "final_category" in df.columns:
            category_counts = df["final_category"].value_counts().head(10).reset_index()
            category_counts.columns = ["category", "count"]
            
            fig = px.bar(
                category_counts,
                x="count",
                y="category",
                orientation="h",
                labels={"category": "Category", "count": "Count"},
                color="count",
                color_continuous_scale="Blues",
            )
            fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No category data available")
    
    # Phase bar chart
    with chart_cols[2]:
        st.subheader("Incidents by Phase")
        if "phase" in df.columns:
            phase_counts = df["phase"].value_counts().reset_index()
            phase_counts.columns = ["phase", "count"]
            
            fig = px.bar(
                phase_counts,
                x="phase",
                y="count",
                labels={"phase": "Phase of Flight", "count": "Count"},
                color="count",
                color_continuous_scale="Oranges",
            )
            fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No phase data available")


# ============================================================================
# Main Application
# ============================================================================

def main():
    """
    Main application entry point.
    """
    # Page configuration
    st.set_page_config(
        page_title="Aviation Safety Incidents Dashboard",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Apply custom CSS for styling
    st.markdown(
        f"""
        <style>
        .main {{
            background-color: {COLORS["background"]};
        }}
        .stMetric {{
            background-color: white;
            padding: 10px;
            border-radius: 5px;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )
    
    # Render header
    render_header()
    
    # Data loading
    st.sidebar.header("Data Loading")
    
    if st.sidebar.button("Load Data", type="primary"):
        with st.spinner("Fetching data from API..."):
            # Fetch classification results
            classification_df = fetch_classification_results(BASE_API_URL)
            
            if not classification_df.empty:
                # Enrich with origin data
                enriched_df = enrich_with_origin(classification_df, BASE_API_URL)
                
                # Prepare canonical DataFrame
                canonical_df = prepare_canonical_dataframe(enriched_df)
                
                # Store in session state
                st.session_state["data"] = canonical_df
                st.session_state["data_loaded"] = True
                
                st.sidebar.success(f"Loaded {len(canonical_df)} records")
            else:
                st.sidebar.error("No data available from API")
                st.session_state["data_loaded"] = False
    
    # Check if data is loaded
    if "data_loaded" not in st.session_state or not st.session_state.get("data_loaded"):
        st.info("👈 Click 'Load Data' in the sidebar to begin")
        return
    
    df = st.session_state.get("data", pd.DataFrame())
    
    if df.empty:
        st.warning("No data available. Please load data from the sidebar.")
        return
    
    # Render filters
    filters = render_sidebar_filters(df)
    
    # Apply filters
    filtered_df = apply_filters(df, filters)
    
    # Render KPIs
    st.markdown("---")
    render_kpis(filtered_df)
    
    # Render time series
    st.markdown("---")
    render_time_series(filtered_df)
    
    # Render map
    st.markdown("---")
    render_map(filtered_df)
    
    # Render bar charts
    st.markdown("---")
    render_bar_charts(filtered_df)


if __name__ == "__main__":
    main()

