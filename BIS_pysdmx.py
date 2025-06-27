import pandas as pd
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import json

try:
    import pysdmx
except ImportError:
    raise ImportError("You must install pysdmx (pip install pysdmx) to use this script.")

try:
    from plot import plot_ts
    import plotly.utils
except ImportError:
    plot_ts = None
    plotly = None

app = FastAPI(
    title="BIS Data Backend (pysdmx)",
    description="Backend serving BIS SDMX data using pysdmx",
    version="0.2",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def fetch_bis_sdmx(resource_id: str, key: str) -> pd.DataFrame:
    # Compose SDMX URL (BIS uses SDMX REST API)
    # Example: https://stats.bis.org/api/v1/data/BIS/WS_TC/Q.CN+XM+JP+US.N.A.M.USD.A?format=sdmx-2.1.0
    sdmx_url = (
        f"https://stats.bis.org/api/v1/data/BIS/"
        f"{resource_id}/{key}?format=sdmx-2.1.0"
    )
    try:
        ds = pysdmx.read_sdmx(sdmx_url)
        df = ds.to_pandas()
        if isinstance(df, dict):
            # Some SDMX packages return a dict of dataframes
            # We'll take the first one
            df = next(iter(df.values()))
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SDMX fetch failed: {str(e)}")

@app.get("/bis_credit_table")
def bis_credit_table(
    resource_id: str = Query("WS_TC"),
    key: str = Query("Q.CN+XM+JP+US.N.A.M.USD.A")
):
    df = fetch_bis_sdmx(resource_id, key)
    if df is None or df.empty:
        return JSONResponse(content={"error": "No data returned from BIS."}, status_code=404)

    # Standardize output
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()
    elif not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)

    result = df.rename(
        columns=lambda x: str(x)
    ).to_dict(orient="records")
    return result

@app.get("/bis_credit_chart")
def bis_credit_chart(
    resource_id: str = Query("WS_TC"),
    key: str = Query("Q.CN+XM+JP+US.N.A.M.USD.A"),
    units: str = Query("USD bn"),
    theme: str = Query("light"),
    startdate: str = Query(None, description="Filter data from this date (yyyy-mm-dd)"),
    mode: str = Query("total", description="total, yoy, or qoq"),
):
    df = fetch_bis_sdmx(resource_id, key)
    if df is None or df.empty:
        return JSONResponse(content={"error": "No data returned from BIS."}, status_code=404)

    # Clean/reshape for plotting
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()
    # Try to find date and country columns
    date_col = [c for c in df.columns if "date" in str(c).lower() or "time" in str(c).lower()]
    cty_col = [c for c in df.columns if "country" in str(c).lower() or "cty" in str(c).lower()]
    value_col = [c for c in df.columns if "value" in str(c).lower() or "obs_value" in str(c).lower()]
    if not date_col or not cty_col or not value_col:
        # Fallback to first three columns
        date_col = [df.columns[0]]
        cty_col = [df.columns[1]]
        value_col = [df.columns[2]]

    df = df[[date_col[0], cty_col[0], value_col[0]]].rename(
        columns={date_col[0]: "Date", cty_col[0]: "Country", value_col[0]: "Value"}
    )
    df['Date'] = pd.PeriodIndex(df['Date'].astype(str), freq='Q').to_timestamp()
    df = df.sort_values("Date")
    if startdate:
        try:
            start = pd.to_datetime(startdate)
            df = df[df['Date'] >= start]
        except Exception as e:
            return JSONResponse(content={"error": f"Invalid startdate: {e}"}, status_code=400)
    df_pivot = df.pivot(index="Date", columns="Country", values="Value").sort_index()

    if mode == "yoy":
        df_pivot = df_pivot.pct_change(periods=4)
        units = "YoY % change"
    elif mode == "qoq":
        df_pivot = df_pivot.pct_change(periods=1)
        units = "QoQ % change"

    if plot_ts is None or plotly is None:
        return JSONResponse(
            content={"error": "plot_ts or plotly not available. Please install plot.py and plotly."},
            status_code=500,
        )

    fig = plot_ts(df_pivot, nome="BIS Data", units=units, theme=theme)
    return JSONResponse(content=json.loads(json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)))

@app.get("/widgets.json")
def get_widgets():
    return {
        "bis_credit_chart": {
            "name": "BIS Chart",
            "description": "Plotly chart of BIS credit data for multiple countries",
            "type": "chart",
            "endpoint": "bis_credit_chart",
            "gridData": {"w": 20, "h": 13},
            "params": [
                {"paramName": "resource_id", "type": "text", "default": "WS_TC", "description": "Resource ID"},
                {"paramName": "key", "type": "text", "default": "Q.CN+XM+JP+US.N.A.M.USD.A", "description": "Key (e.g., Q.US.N.A.M.XDC.U or Q.US+ES.N.A.M.XDC.U for multiple countries)"},
                {"paramName": "units", "type": "text", "default": "USD bn", "description": "Units label"},
                {"paramName": "startdate", "type": "date", "default": "", "description": "Start date (yyyy-mm-dd)"},
                {"paramName": "mode", "type": "text", "default": "total", "description": "Display mode", "options": [
                    {"value": "total", "label": "Total Outstanding"},
                    {"value": "yoy", "label": "Year-on-Year Change"},
                    {"value": "qoq", "label": "Quarterly Change"}
                ]}
            ]
        },
        "bis_credit_table": {
            "name": "BIS Credit Data Table",
            "description": "Tabular view of BIS credit data time series",
            "type": "table",
            "endpoint": "bis_credit_table",
            "gridData": {"w": 20, "h": 13},
            "params": [
                {"paramName": "resource_id", "type": "text", "default": "WS_TC", "description": "Resource ID"},
                {"paramName": "key", "type": "text", "default": "Q.CN+XM+JP+US.N.A.M.USD.A", "description": "Key (e.g., Q.US.N.A.M.XDC.U or Q.US+ES.N.A.M.XDC.U for multiple countries)"}
            ]
        }
    }

@app.get("/")
def root():
    return {"message": "BIS Data Backend for OpenBB Workspace (pysdmx)"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8800)