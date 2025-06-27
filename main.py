import xml.etree.ElementTree as ET
import requests
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from functools import wraps
import asyncio

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

# --- Widget Endpoint ---
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
                "Date": row["Date"],          # or row["time_period"], depending on parser
                "Country": row["Country"],
                "Value": row["Value"]
            })
        except KeyError as e:
            # Log or handle missing key
            print(f"Missing key in row: {e}")
            continue
    return table


# --- Widgets Metadata Endpoint ---
@app.get("/widgets.json")
def get_widgets():
    return WIDGETS

# --- Root Endpoint (optional) ---
@app.get("/")
def root():
    return {"message": "BIS Data Backend for OpenBB Workspace"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8800)
