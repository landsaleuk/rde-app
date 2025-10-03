import os, math, csv
import psycopg2, psycopg2.extras
from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse, JSONResponse

DATABASE_URL = os.getenv("DATABASE_URL","postgresql://postgres:postgres@db:5432/land")
app = FastAPI(title="RDE API")

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

@app.get("/healthz")
def healthz():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            return {"ok": True}

@app.get("/api/cohorts/stats")
def cohort_stats():
    sql = """
      SELECT cohort, COUNT(*)::bigint AS n
      FROM target_cohorts
      WHERE cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate')
      GROUP BY cohort
    """
    agg = {"A_bare_land":0,"B_single_holding":0,"C_dispersed_estate":0}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for r in cur.fetchall():
                agg[r["cohort"]] = int(r["n"])
    agg["total"] = sum(agg.values())
    return agg

def _where(params, values):
    where = ["cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate')"]
    if params.get("min_acres") is not None: where.append("acres >= %s"); values.append(params["min_acres"])
    if params.get("max_acres") is not None: where.append("acres <= %s"); values.append(params["max_acres"])
    if params.get("min_uprn_interior") is not None: where.append("uprn_interior_count >= %s"); values.append(params["min_uprn_interior"])
    if params.get("max_uprn_interior") is not None: where.append("uprn_interior_count <= %s"); values.append(params["max_uprn_interior"])
    if params.get("min_uprn_total") is not None: where.append("uprn_count >= %s"); values.append(params["min_uprn_total"])
    if params.get("max_uprn_total") is not None: where.append("uprn_count <= %s"); values.append(params["max_uprn_total"])
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
    allowed = {"parcel_id","cohort","acres","uprn_count","uprn_interior_count"}
    if sort_by not in allowed: sort_by = "parcel_id"
    sort_dir = "desc" if sort_dir.lower()=="desc" else "asc"

    params = locals()
    values = []
    where_sql = _where(params, values)
    offset = (page-1)*page_size

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
    return {"page":page,"page_size":page_size,"total":total,"pages":math.ceil(total/page_size),"items":rows}

@app.get("/api/parcels.csv")
def parcels_csv(
    min_acres: float | None = None,
    max_acres: float | None = None,
    min_uprn_interior: int | None = None,
    max_uprn_interior: int | None = None,
    min_uprn_total: int | None = None,
    max_uprn_total: int | None = None
):
    params = locals()
    values = []
    where_sql = _where(params, values)
    def stream():
        yield "parcel_id,cohort,acres,uprn_total,uprn_interior,uprn_boundary\n"
        with get_conn() as conn:
            with conn.cursor(name="csv_cur") as cur:
                cur.itersize = 10000
                cur.execute(f"""
                    SELECT parcel_id, cohort, acres,
                           uprn_count AS uprn_total,
                           uprn_interior_count AS uprn_interior,
                           (uprn_count - uprn_interior_count) AS uprn_boundary
                    FROM cohort_parcels_map
                    {where_sql}
                    ORDER BY parcel_id
                """, values)
                for r in cur:
                    yield f'{r["parcel_id"]},{r["cohort"]},{r["acres"]},{r["uprn_total"]},{r["uprn_interior"]},{r["uprn_boundary"]}\n'
    return StreamingResponse(stream(), media_type="text/csv",
        headers={"Content-Disposition":"attachment; filename=parcels.csv"})

@app.get("/api/parcels/{parcel_id}/uprns")
def parcel_uprns(parcel_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Prefer precomputed interior if available
            try:
                cur.execute("""
                    SELECT b.uprn,
                           (CASE WHEN ST_Covers(i.i_geom, b.ugeom) THEN TRUE ELSE FALSE END) AS interior,
                           ST_X(ST_Transform(b.ugeom,4326)) AS lon,
                           ST_Y(ST_Transform(b.ugeom,4326)) AS lat
                    FROM parcel_uprn_base b
                    JOIN parcel_interior i USING (parcel_id)
                    WHERE b.parcel_id=%s
                    ORDER BY b.uprn
                """, (parcel_id,))
                return {"parcel_id": parcel_id, "uprns": cur.fetchall()}
            except Exception:
                cur.execute("""
                    WITH p AS (
                      SELECT geom, ST_Buffer(geom,-5) AS i_geom
                      FROM parcel_1acre WHERE parcel_id=%s
                    )
                    SELECT u.uprn,
                           (CASE WHEN NOT ST_IsEmpty(p.i_geom) AND ST_Covers(p.i_geom,u.geom) THEN TRUE ELSE FALSE END) AS interior,
                           ST_X(ST_Transform(u.geom,4326)) AS lon,
                           ST_Y(ST_Transform(u.geom,4326)) AS lat
                    FROM os_open_uprn u, p
                    WHERE ST_Covers(p.geom,u.geom)
                    ORDER BY u.uprn
                """, (parcel_id,))
                return {"parcel_id": parcel_id, "uprns": cur.fetchall()}