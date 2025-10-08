import os
import math
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/land")

app = FastAPI(title="Rural Data Engine – Parcels Admin")

# allow the static site (8080) to call the API (8000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://51.79.65.16:8080",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)

templates = Jinja2Templates(directory="templates")

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

@app.get("/")
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

def build_where(params, values):
    where = ["cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate')"]
    if params.get("min_acres") is not None:
        where.append("acres >= %s"); values.append(params["min_acres"])
    if params.get("max_acres") is not None:
        where.append("acres <= %s"); values.append(params["max_acres"])
    if params.get("min_uprn_interior") is not None:
        where.append("uprn_interior_count >= %s"); values.append(params["min_uprn_interior"])
    if params.get("max_uprn_interior") is not None:
        where.append("uprn_interior_count <= %s"); values.append(params["max_uprn_interior"])
    if params.get("min_uprn_total") is not None:
        where.append("uprn_count >= %s"); values.append(params["min_uprn_total"])
    if params.get("max_uprn_total") is not None:
        where.append("uprn_count <= %s"); values.append(params["max_uprn_total"])
    return "WHERE " + " AND ".join(where)

@app.get("/api/parcels")
def list_parcels(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    min_acres: float | None = None,
    max_acres: float | None = None,
    min_uprn_interior: int | None = None,
    max_uprn_interior: int | None = None,
    min_uprn_total: int | None = None,
    max_uprn_total: int | None = None,
    sort_by: str = Query("parcel_id"),
    sort_dir: str = Query("asc")
):
    allowed_sorts = {"parcel_id","cohort","acres","uprn_count","uprn_interior_count"}
    if sort_by not in allowed_sorts:
        sort_by = "parcel_id"
    sort_dir = "desc" if sort_dir.lower() == "desc" else "asc"

    params = {
        "min_acres": min_acres,
        "max_acres": max_acres,
        "min_uprn_interior": min_uprn_interior,
        "max_uprn_interior": max_uprn_interior,
        "min_uprn_total": min_uprn_total,
        "max_uprn_total": max_uprn_total,
    }
    values = []
    where_sql = build_where(params, values)

    offset = (page - 1) * page_size
    sql_count = f"SELECT COUNT(*) AS n FROM cohort_parcels_map {where_sql};"
    sql_page = f"""
        SELECT parcel_id, cohort, acres,
               uprn_count AS uprn_total,
               uprn_interior_count AS uprn_interior,
               (uprn_count - uprn_interior_count) AS uprn_boundary
        FROM cohort_parcels_map
        {where_sql}
        ORDER BY {sort_by} {sort_dir}
        LIMIT %s OFFSET %s;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_count, values)
            total = cur.fetchone()["n"]

            cur.execute(sql_page, values + [page_size, offset])
            rows = cur.fetchall()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "pages": math.ceil(total / page_size) if page_size else 1,
        "items": rows
    }

@app.get("/api/parcels.csv")
def parcels_csv(
    min_acres: float | None = None,
    max_acres: float | None = None,
    min_uprn_interior: int | None = None,
    max_uprn_interior: int | None = None,
    min_uprn_total: int | None = None,
    max_uprn_total: int | None = None
):
    params = {
        "min_acres": min_acres,
        "max_acres": max_acres,
        "min_uprn_interior": min_uprn_interior,
        "max_uprn_interior": max_uprn_interior,
        "min_uprn_total": min_uprn_total,
        "max_uprn_total": max_uprn_total,
    }
    values = []
    where_sql = build_where(params, values)

    sql = f"""
        COPY (
            SELECT parcel_id, cohort, acres,
                   uprn_count AS uprn_total,
                   uprn_interior_count AS uprn_interior,
                   (uprn_count - uprn_interior_count) AS uprn_boundary
            FROM cohort_parcels_map
            {where_sql}
            ORDER BY parcel_id
        ) TO STDOUT WITH CSV HEADER
    """

    def stream():
        with get_conn() as conn:
            with conn.cursor() as cur:
                # psycopg2 COPY requires mogrify for embedded params; build temp table is overkill.
                # Here, we execute using server-side PREPARE via format — but since we already applied params
                # in WHERE via bind values, we'll inject literals safely by relying on the cursor's mogrify where needed.
                # Simpler approach: run a normal SELECT and yield CSV. Let's do that for compatibility:
                pass

    # Simpler streaming without COPY:
    import csv, io
    def stream_select():
        header = ["parcel_id","cohort","acres","uprn_total","uprn_interior","uprn_boundary"]
        yield ",".join(header) + "\n"
        chunk = 10000
        with get_conn() as conn:
            with conn.cursor(name="csv_cur") as cur:  # server-side cursor to stream rows
                cur.itersize = chunk
                cur.execute(f"""
                    SELECT parcel_id, cohort, acres,
                           uprn_count AS uprn_total,
                           uprn_interior_count AS uprn_interior,
                           (uprn_count - uprn_interior_count) AS uprn_boundary
                    FROM cohort_parcels_map
                    {where_sql}
                    ORDER BY parcel_id
                """, values)
                for rec in cur:
                    yield f'{rec["parcel_id"]},{rec["cohort"]},{rec["acres"]},{rec["uprn_total"]},{rec["uprn_interior"]},{rec["uprn_boundary"]}\n'

    return StreamingResponse(stream_select(), media_type="text/csv",
                             headers={"Content-Disposition":"attachment; filename=parcels.csv"})

@app.get("/api/parcels/{parcel_id}/uprns")
def parcel_uprns(parcel_id: int):
    """
    Return UPRNs inside a parcel with an 'interior' boolean and WGS84 coords.
    Uses parcel_uprn_base + parcel_interior if available; falls back to buffer compute.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Try with parcel_interior if present
            try:
                cur.execute("""
                    SELECT b.uprn,
                           (CASE WHEN ST_Covers(i.i_geom, b.ugeom) THEN TRUE ELSE FALSE END) AS interior,
                           ST_X(ST_Transform(b.ugeom,4326)) AS lon,
                           ST_Y(ST_Transform(b.ugeom,4326)) AS lat
                    FROM parcel_uprn_base b
                    JOIN parcel_interior i USING (parcel_id)
                    WHERE b.parcel_id = %s
                    ORDER BY b.uprn;
                """, (parcel_id,))
                rows = cur.fetchall()
                return {"parcel_id": parcel_id, "uprns": rows}
            except Exception:
                # Fallback: compute interior on the fly
                cur.execute("""
                    WITH p AS (
                      SELECT geom, ST_Buffer(geom, -5) AS i_geom
                      FROM parcel_1acre WHERE parcel_id = %s
                    )
                    SELECT u.uprn,
                           (CASE WHEN NOT ST_IsEmpty(p.i_geom) AND ST_Covers(p.i_geom, u.geom)
                                 THEN TRUE ELSE FALSE END) AS interior,
                           ST_X(ST_Transform(u.geom,4326)) AS lon,
                           ST_Y(ST_Transform(u.geom,4326)) AS lat
                    FROM os_open_uprn u, p
                    WHERE ST_Covers(p.geom, u.geom)
                    ORDER BY u.uprn;
                """, (parcel_id,))
                rows = cur.fetchall()
                return {"parcel_id": parcel_id, "uprns": rows}

@app.get("/api/cohorts/stats")
def cohort_stats():
    sql = """
      SELECT cohort, COUNT(*)::bigint AS n
      FROM target_cohorts
      WHERE cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate')
      GROUP BY cohort
    """
    agg = {"A_bare_land": 0, "B_single_holding": 0, "C_dispersed_estate": 0}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for r in cur.fetchall():
                agg[r["cohort"]] = int(r["n"])
    agg["total"] = sum(agg.values())
    return agg

@app.get("/api/cohort-counts")
def cohort_counts_alias():
    # backwards-compatible alias for the HTML page
    return cohort_stats()

@app.get("/stats/nonland")
def nonland_stats():
    """
    Return total excluded parcels and counts by reason.
    Prefer the light map MV (nonland_parcels_map); if missing, derive from flags.
    """
    fast_sql = """
        SELECT COALESCE(nonland_reason,'other') AS reason,
               COUNT(*)::bigint AS n
        FROM nonland_parcels_map
        GROUP BY COALESCE(nonland_reason,'other')
        ORDER BY reason;
    """

    fallback_sql = """
        WITH reasons_ranked AS (
          SELECT
            parcel_id,
            COALESCE(nonland_reason,'other') AS reason,
            CASE
              WHEN nonland_reason = 'offshore/land<10%' THEN 1
              WHEN nonland_reason = 'water>=50%'        THEN 2
              WHEN nonland_reason = 'road corridor'     THEN 3
              WHEN nonland_reason = 'rail corridor'     THEN 4
              WHEN nonland_reason = 'long-thin'         THEN 5
              ELSE 99
            END AS pri
          FROM parcel_nonland_flags
          WHERE is_nonland
        ),
        primary_reason AS (
          SELECT DISTINCT ON (parcel_id) parcel_id, reason
          FROM reasons_ranked
          ORDER BY parcel_id, pri
        )
        SELECT reason, COUNT(*)::bigint AS n
        FROM primary_reason
        GROUP BY reason
        ORDER BY reason;
    """

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(fast_sql)
                rows = cur.fetchall()
    except Exception:
        # Fallback: compute from flags (works even if the MV hasn’t been created yet)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(fallback_sql)
                rows = cur.fetchall()

    total = sum(int(r["n"]) for r in rows)
    return {
        "total": total,
        "by_reason": [{"reason": r["reason"], "count": int(r["n"])} for r in rows],
    }