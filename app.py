import duckdb
import polars as pl
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import os, json

# Ruta absoluta hacia el Parquet fuera del directorio frontend
PARQUET_PATH = r"D:\0Dev\hugeXml\bays.parquet"

# ============================================
# Cargar DuckDB
# ============================================

con = duckdb.connect()

con.execute(f"""
    CREATE OR REPLACE TABLE bays AS 
    SELECT * FROM '{PARQUET_PATH}'
""")

app = FastAPI(
    title="XML BAY ANALYZER API",
    version="1.0"
)

# ============================================
# Frontend
# ============================================

app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")


@app.get("/")
def root():
    return {"status": "ok", "message": "DuckDB funcionando"}


@app.get("/bays")
def list_bays(limit: int = 50):
    df = con.execute(f"SELECT BayName FROM bays LIMIT {limit}").df()
    return df.to_dict(orient="records")


@app.get("/bay/{bayname}")
def get_bay(bayname: str):
    df = con.execute("SELECT * FROM bays WHERE BayName = ?", [bayname]).df()
    if df.empty:
        return {"error": "Bay no encontrada"}
    return df.to_dict(orient="records")[0]


@app.get("/bay", response_class=HTMLResponse)
def get_bay_htmx(bayname: str):
    df = con.execute("SELECT * FROM bays WHERE BayName = ?", [bayname]).df()
    if df.empty:
        return "<p class='text-red-400'>No encontrado</p>"

    row = df.to_dict(orient="records")[0]

    return f"""
    <h2 class="text-xl text-green-400 mb-4">Bay: {row['BayName']}</h2>
    <pre class="bg-gray-700 p-4 rounded text-sm">{json.dumps(row, indent=2)}</pre>
    """
