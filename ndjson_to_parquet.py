#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ndjson_to_parquet_duckdb.py
Convierte NDJSON → Parquet usando DuckDB, excelente para JSON heterogéneo.

Uso:
    python ndjson_to_parquet_duckdb.py --in bays.jsonl --out bays.parquet
"""

import argparse
import duckdb
from tqdm import tqdm
import os


def ndjson_to_parquet(in_path, out_path):
    print(f"[INFO] Analizando NDJSON con DuckDB...")

    con = duckdb.connect()

    # Crear tabla virtual desde NDJSON streaming
    con.execute(f"""
        CREATE OR REPLACE TABLE bays AS
        SELECT * FROM read_json_auto('{in_path}', format='newline_delimited');
    """)

    print(f"[INFO] Escribiendo Parquet optimizado...")

    con.execute(f"""
        COPY bays TO '{out_path}' (FORMAT PARQUET);
    """)

    print(f"[OK] Parquet generado: {out_path}")

    # Opcional: contar registros
    count = con.execute("SELECT COUNT(*) FROM bays").fetchone()[0]
    print(f"[OK] Registros en Parquet: {count}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="input", required=True)
    parser.add_argument("--out", dest="output", default="bays.parquet")
    args = parser.parse_args()

    ndjson_to_parquet(args.input, args.output)


if __name__ == "__main__":
    main()
