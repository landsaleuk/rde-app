from fastapi import FastAPI
import os
import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@db:5432/land")

app = FastAPI()

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/cohort-counts")
def cohort_counts():
    """
    Returns the global count of parcels per cohort (A/B/C only).
    Uses target_cohorts for speed (indexed on cohort).
    """
    sql = """
        SELECT cohort, COUNT(*)::bigint AS n
        FROM target_cohorts
        WHERE cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate')
        GROUP BY cohort;
    """
    out = {"A_bare_land": 0, "B_single_holding": 0, "C_dispersed_estate": 0}
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for cohort, n in cur.fetchall():
                out[cohort] = int(n)
    out["total"] = sum(out.values())
    return out
