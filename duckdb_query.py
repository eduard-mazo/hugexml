#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
duckdb_query.py
Consulta archivos Parquet utilizando DuckDB desde Python.

Uso:
    python duckdb_query.py --parquet bays.parquet --sql "SELECT * FROM bays LIMIT 10"
"""

import argparse
import duckdb
import polars as pl


def query_duckdb(parquet_path, sql):
    print(f"[DuckDB] Ejecutando consulta sobre: {parquet_path}")
    
    con = duckdb.connect()

    # Registrar el parquet como tabla virtual
    con.execute(f"CREATE OR REPLACE TABLE bays AS SELECT * FROM '{parquet_path}'")

    # Ejecutar la consulta
    result = con.execute(sql).df()
    print(result)

    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", required=True, help="Ruta al archivo Parquet")
    parser.add_argument("--sql", required=True, help="Consulta SQL")
    parser.add_argument("--out", help="Exportar resultado a CSV (opcional)")
    args = parser.parse_args()

    result = query_duckdb(args.parquet, args.sql)

    if args.out:
        df = pl.DataFrame(result)
        df.write_csv(args.out)
        print(f"CSV exportado a {args.out}")


if __name__ == "__main__":
    main()
