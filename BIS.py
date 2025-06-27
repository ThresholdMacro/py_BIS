import xml.etree.ElementTree as ET
import requests
import pandas as pd
import numpy as np
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from functools import wraps
import asyncio
import json

# If you have 'plot_ts' and 'plotly' from plot.py, import here
try:
    from plot import plot_ts
    import plotly.utils
except ImportError:
    plot_ts = None
    plotly = None

# --- Widget Registry ---
WIDGETS = {}

def register_widget(widget_config):
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await func(*args, **kwargs)
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        endpoint = widget_config.get("endpoint")
        if endpoint:
            if "id" not in widget_config:
                widget_config["id"] = endpoint
            WIDGETS[endpoint] = widget_config

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    return decorator

# --- BIS XML Parsing ---
def parse_bis_xml(xml_text):
    root = ET.fromstring(xml_text)
    data = []
    for series in root.findall('.//Series'):
        country_code = series.get('BORROWERS_CTY', 'Unknown')
        for obs in series.findall('Obs'):
            data.append({
                'Date': obs.get('TIME_PERIOD'),
                'Value': float(obs.get('OBS_VALUE')) if obs.get('OBS_VALUE') else None,
                'Country': country_code
            })
    return data

def parse_bis_xml_df(xml_text):
    """
    Returns DataFrame for use in chart endpoint (for pivoting, etc.)
    """
    root = ET.fromstring(xml_text)
    data = []
    for series in root.findall('.//Series'):
        country_code = series.get('BORROWERS_CTY', 'Unknown')
        for obs in series.findall('Obs'):
            data.append({
                'Date': obs.get('TIME_PERIOD'),
                'Value': float(obs.get('OBS_VALUE')) if obs.get('OBS_VALUE') else None,
                'Country': country_code
            })
    return pd.DataFrame(data)

# --- BIS Data Fetching ---
def fetch_bis_data(context: str, agency_id: str, resource_id: str, version: str, key: str):
    try:
        url = f"https://stats.bis.org/api/v2/data/{context}/{agency_id}/{resource_id}/{version}/{key}"
        headers = {'Accept': 'application/xml'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return parse_bis_xml(response.text)
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error fetching BIS series: {str(e)}")

def fetch_bis_data_simple(resource_id: str, key: str):
    url = f"https://stats.bis.org/api/v2/data/dataflow/BIS/{resource_id}/+/{key}"
    headers = {'Accept': 'application/xml'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text

# --- FastAPI App ---
app = FastAPI(
    title="BIS Data Backend",
    description="Backend serving BIS data as OpenBB Workspace widgets",
    version="0.1",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Widget Endpoint (table) ---
@register_widget({
    "name": "BIS Credit Data Table",
    "description": "Tabular view of BIS credit data time series",
    "type": "table",
    "endpoint": "bis_credit_table",
    "gridData": {"w": 20, "h": 13},
    "params": [
        {
            "paramName": "resource_id",
            "type": "text",
            "default": "WS_TC",
            "description": "Resource ID",
            "options": [
                {"value": "WS_TC", "label": "Total credit to non-financial sector"}
            ]
        },
        {
            "paramName": "key",
            "type": "text",
            "default": "",
            "description": "Key",
            "options": [
                {"value": "Q..N.A.M.USD.A", "label": "All"},
                {"value": "Q.AU.N.A.M.USD.A", "label": "Australia"},
                {"value": "Q.CA.N.A.M.USD.A", "label": "Canada"},
                {"value": "Q.CN.N.A.M.USD.A", "label": "China"},
                {"value": "Q.XM.N.A.M.USD.A", "label": "EuroArea"},
                {"value": "Q.FR.N.A.M.USD.A", "label": "France"},
                {"value": "Q.DE.N.A.M.USD.A", "label": "Germany"},
                {"value": "Q.IT.N.A.M.USD.A", "label": "Italy"},
                {"value": "Q.JP.N.A.M.USD.A", "label": "Japan"},
                {"value": "Q.ES.N.A.M.USD.A", "label": "Spain"},
                {"value": "Q.GB.N.A.M.USD.A", "label": "United Kingdom"},
                {"value": "Q.US.N.A.M.USD.A", "label": "United States"}
            ]
        }
    ]
})
@app.get("/bis_credit_table")
def bis_credit_table(resource_id: str = Query("WS_TC"), key: str = Query("")):
    data = fetch_bis_data("dataflow", "BIS", resource_id, "+", key)
    table = []
    for row in data:
        try:
            table.append({
                "Date": row["Date"],
                "Country": row["Country"],
                "Value": row["Value"]
            })
        except KeyError as e:
            print(f"Missing key in row: {e}")
            continue
    return table

# --- Chart Endpoint (from chart.py) ---
@app.get("/bis_credit_chart")
def bis_credit_chart(
    resource_id: str = Query("WS_TC"),
    key: str = Query("Q.CN+XM+JP+US.N.A.M.USD.A"),
    units: str = Query("USD bn"),
    theme: str = Query("light"),
    startdate: str = Query(None, description="Filter data from this date (yyyy-mm-dd)"),
    mode: str = Query("total", description="total, yoy, or qoq"),
):
    xml_text = fetch_bis_data_simple(resource_id, key)
    df = parse_bis_xml_df(xml_text)
    if df.empty:
        return JSONResponse(content={"error": "No data returned from BIS."}, status_code=404)

    # Convert Date to pandas Period for sorting and filtering
    df['Date'] = pd.PeriodIndex(df['Date'], freq='Q').to_timestamp()
    df = df.sort_values("Date")

    # Filter by startdate if provided
    if startdate:
        try:
            start = pd.to_datetime(startdate)
            df = df[df['Date'] >= start]
        except Exception as e:
            return JSONResponse(content={"error": f"Invalid startdate: {e}"}, status_code=400)

    # Pivot for plotting (Date as index, countries as columns)
    df_pivot = df.pivot(index="Date", columns="Country", values="Value").sort_index()

    # Apply mode transformation
    if mode == "yoy":
        df_pivot = df_pivot.pct_change(periods=4)
        units = "YoY % change"
    elif mode == "qoq":
        df_pivot = df_pivot.pct_change(periods=1)
        units = "QoQ % change"
    else:
        units = units  # "total outstanding"

    if plot_ts is None or plotly is None:
        return JSONResponse(
            content={"error": "plot_ts or plotly not available. Please install plot.py and plotly."},
            status_code=500,
        )

    fig = plot_ts(df_pivot, nome="BIS Data", units=units, theme=theme)
    return JSONResponse(content=json.loads(json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)))

# --- Widgets Metadata Endpoint ---
@app.get("/widgets.json")
def get_widgets():
    # Combine both chart and table widgets
    chart_widget = {
        "name": "BIS Chart",
        "description": "Plotly chart of BIS credit data for multiple countries",
        "type": "chart",
        "endpoint": "bis_credit_chart",
        "gridData": {"w": 20, "h": 13},
        "params": [
            {
                "paramName": "resource_id",
                "type": "text",
                "default": "WS_TC",
                "description": "Resource ID"
            },
            {
                "paramName": "key",
                "type": "text",
                "default": "Q.CN+XM+JP+US.N.A.M.USD.A",
                "description": "Key (e.g., Q.US.N.A.M.XDC.U or Q.US+ES.N.A.M.XDC.U for multiple countries)"
            },
            {
                "paramName": "units",
                "type": "text",
                "default": "USD bn",
                "description": "Units label"
            },
            {
                "paramName": "startdate",
                "type": "date",
                "default": "",
                "description": "Start date (yyyy-mm-dd)"
            },
            {
                "paramName": "mode",
                "type": "text",
                "default": "total",
                "description": "Display mode",
                "options": [
                    {"value": "total", "label": "Total Outstanding"},
                    {"value": "yoy", "label": "Year-on-Year Change"},
                    {"value": "qoq", "label": "Quarterly Change"}
                ]
            }
        ]
    }
    widgets = dict(WIDGETS)
    widgets["bis_credit_chart"] = chart_widget
    return widgets

# --- Root Endpoint ---
@app.get("/")
def root():
    return {"message": "BIS Data Backend for OpenBB Workspace"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8800)