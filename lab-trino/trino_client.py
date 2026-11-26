"""Trino connection and the sql() helper of the lab, shared by every script."""
import os

import pandas as pd
import trino

CATALOG = "lakehouse"

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 20)
pd.set_option("display.max_colwidth", 80)

conn = trino.dbapi.connect(
    host=os.environ.get("TRINO_HOST", "trino"),
    port=int(os.environ.get("TRINO_PORT", "8080")),
    user=os.environ.get("TRINO_USER", os.environ.get("KUBERNETES_NAMESPACE", "lab")),
    http_scheme="http",
    # Default catalog: unqualified names resolve to lakehouse.<schema>.<table>
    catalog=CATALOG,
)


def sql(statement):
    """Run a statement and fetch its result: the query only completes once fetched."""
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)


def show(title, statement):
    """Print a title, then the result of the statement."""
    print(f"\n=== {title}")
    df = sql(statement)
    print(df.to_string(index=False) if not df.empty else "(no rows)")
    return df


def value(statement):
    """First cell of the result, as a Python value."""
    v = sql(statement).iloc[0, 0]
    return v.item() if hasattr(v, "item") else v
