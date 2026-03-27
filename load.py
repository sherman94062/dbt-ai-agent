import duckdb
import psycopg2
from io import StringIO

duck = duckdb.connect()
duck.execute("INSTALL tpch; LOAD tpch;")
duck.execute("CALL dbgen(sf=0.1);")

pg = psycopg2.connect("postgresql://arthursherman@localhost:5432/tpch")
pg.autocommit = True
cur = pg.cursor()

tables = ["region", "nation", "customer", "supplier", "part", "partsupp", "orders", "lineitem"]

type_map = {"INTEGER": "INTEGER", "BIGINT": "BIGINT", "VARCHAR": "TEXT",
            "DOUBLE": "NUMERIC(15,2)", "DATE": "DATE", "DECIMAL": "NUMERIC(15,2)"}

for table in tables:
    print(f"Loading {table}...")
    df = duck.execute(f"SELECT * FROM {table}").df()

    cols = duck.execute(f"DESCRIBE {table}").fetchall()
    col_defs = ", ".join(f"{c[0]} {type_map.get(c[1], 'TEXT')}" for c in cols)
    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
    cur.execute(f"CREATE TABLE {table} ({col_defs});")

    # Use tab-separated to avoid comma conflicts in text fields
    buf = StringIO()
    df.to_csv(buf, index=False, header=False, sep="\t")
    buf.seek(0)
    cur.copy_from(buf, table, sep="\t", null="")
    print(f"  {len(df):,} rows loaded")

duck.close()
pg.close()
print("Done!")
