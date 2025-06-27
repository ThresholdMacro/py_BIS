from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import requests
import xml.etree.ElementTree as ET

app = FastAPI()

def parse_bis_xml(xml_text):
    root = ET.fromstring(xml_text)
    data = []
    for series in root.findall('.//Series'):
        for obs in series.findall('Obs'):
            data.append({
                'time_period': obs.get('TIME_PERIOD'),
                'value': obs.get('OBS_VALUE')
            })
    return data
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
        for obs in series.findall('Obs'):
            data.append({
                'time_period': obs.get('TIME_PERIOD'),
                'value': float(obs.get('OBS_VALUE')) if obs.get('OBS_VALUE') else None
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
    "gridData": {"w": 12, "h": 8},
    "params": [
        {"name": "context", "type": "string", "default": "dataflow"},
        {"name": "agency_id", "type": "string", "default": "BIS"},
        {"name": "resource_id", "type": "string", "default": "WS_TC"},
        {"name": "version", "type": "string", "default": "+"},
        {"name": "key", "type": "string", "default": "Q.AU.N.A.M.XDC.U"},
    ],
})
@app.get("/bis_credit_table")
def bis_credit_table(
    context: str = Query("dataflow"),
    agency_id: str = Query("BIS"),
    resource_id: str = Query("WS_TC"),
    version: str = Query("+"),
    key: str = Query("Q.AU.N.A.M.XDC.U"),
):
    data = fetch_bis_data(context, agency_id, resource_id, version, key)
    table = []
    for row in data:
        table.append({
            "Date": row["time_period"],
            "Value": row["value"],
        })
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

def fetch_bis_data(context: str, agency_id: str, resource_id: str, version: str, key: str):
    try:
        url = f"https://stats.bis.org/api/v2/data/{context}/{agency_id}/{resource_id}/{version}/{key}"
        headers = {'Accept': 'application/json'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        if 'application/json' in response.headers.get('Content-Type', ''):
            return response.json()
        else:
            return parse_bis_xml(response.text)
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error fetching BIS series: {str(e)}")

@app.get("/bis")
def bis_data(
    context: str = Query("dataflow"),
    agency_id: str = Query("BIS"),
    resource_id: str = Query("WS_TC"),
    version: str = Query("+"),
    key: str = Query("Q.AU.N.A.M.XDC.U")
):
    try:
        data = fetch_bis_data(context, agency_id, resource_id, version, key)
        return JSONResponse(content=data)
    except Exception as e:
        return {"error": str(e)}
