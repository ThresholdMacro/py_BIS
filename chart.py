import xml.etree.ElementTree as ET
import requests
import pandas as pd
import numpy as np
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from plot import plot_ts
import plotly.utils
import json

app = FastAPI(title="BIS Chart Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def fetch_bis_data(resource_id: str, key: str):
    url = f"https://stats.bis.org/api/v2/data/dataflow/BIS/{resource_id}/+/{key}"
    headers = {'Accept': 'application/xml'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text

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
    return pd.DataFrame(data)

@app.get("/bis_credit_chart")
def bis_credit_chart(
    resource_id: str = Query("WS_TC"),
    key: str = Query("Q.CN+XM+JP+US.N.A.M.USD.A"),
    units: str = Query("USD bn"),
    theme: str = Query("light"),
    startdate: str = Query(None, description="Filter data from this date (yyyy-mm-dd)"),
    mode: str = Query("total", description="total, yoy, or qoq"),
):
    xml_text = fetch_bis_data(resource_id, key)
    df = parse_bis_xml(xml_text)
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

    fig = plot_ts(df_pivot, nome="BIS Data", units=units, theme=theme)
    return JSONResponse(content=json.loads(json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)))

@app.get("/widgets.json")
def widgets_json():
    return [
        {
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
    ]

@app.get("/")
def root():
    return {"message": "BIS Credit Chart API is running."}
