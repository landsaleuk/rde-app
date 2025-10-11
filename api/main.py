import os
import math
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query, Request
from fastapi.responses import StreamingResponse
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
    # default: only A/B/C (you can expose X_exclude with a flag later)
    where = ["cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate')"]

    # acres
    if params.get("min_acres") is not None:
        where.append("acres >= %s"); values.append(params["min_acres"])
    if params.get("max_acres") is not None:
        where.append("acres <= %s"); values.append(params["max_acres"])

    # uprns
    if params.get("min_uprn_interior") is not None:
        where.append("uprn_interior_count >= %s"); values.append(params["min_uprn_interior"])
    if params.get("max_uprn_interior") is not None:
        where.append("uprn_interior_count <= %s"); values.append(params["max_uprn_interior"])
    if params.get("min_uprn_total") is not None:
        where.append("uprn_count >= %s"); values.append(params["min_uprn_total"])
    if params.get("max_uprn_total") is not None:
        where.append("uprn_count <= %s"); values.append(params["max_uprn_total"])

    # new metrics
    if params.get("min_water_pct") is not None:
        where.append("water_pct >= %s"); values.append(params["min_water_pct"])
    if params.get("max_water_pct") is not None:
        where.append("water_pct <= %s"); values.append(params["max_water_pct"])
    if params.get("min_land_pct") is not None:
        where.append("land_pct >= %s"); values.append(params["min_land_pct"])
    if params.get("max_land_pct") is not None:
        where.append("land_pct <= %s"); values.append(params["max_land_pct"])

    # boolean “excludes”
    if params.get("exclude_offshore"):
        where.append("NOT is_offshore")
    if params.get("exclude_road_corridor"):
        where.append("NOT is_road_corridor")
    if params.get("exclude_rail_corridor"):
        where.append("NOT is_rail_corridor")
    if params.get("exclude_roadlike"):
        where.append("NOT is_roadlike_longthin")

    return "WHERE " + " AND ".join(where)

@app.get("/api/parcels")
def list_parcels(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    # existing filters
    min_acres: float | None = None,
    max_acres: float | None = None,
    min_uprn_interior: int | None = None,
    max_uprn_interior: int | None = None,
    min_uprn_total: int | None = None,
    max_uprn_total: int | None = None,
    # new filters
    min_water_pct: float | None = None,
    max_water_pct: float | None = None,
    min_land_pct: float | None = None,
    max_land_pct: float | None = None,
    exclude_offshore: bool | None = None,
    exclude_road_corridor: bool | None = None,
    exclude_rail_corridor: bool | None = None,
    exclude_roadlike: bool | None = None,
    # sorting
    sort_by: str = Query("parcel_id"),
    sort_dir: str = Query("asc")
):
    # columns allowed for order-by
    allowed_sorts = {
        "parcel_id","cohort","acres",
        "uprn_count","uprn_interior_count","uprn_boundary_count",
        "water_pct","land_pct"
    }
    if sort_by not in allowed_sorts:
        sort_by = "parcel_id"
    sort_dir = "desc" if sort_dir.lower() == "desc" else "asc"

    params = {
        "min_acres": min_acres, "max_acres": max_acres,
        "min_uprn_interior": min_uprn_interior, "max_uprn_interior": max_uprn_interior,
        "min_uprn_total": min_uprn_total, "max_uprn_total": max_uprn_total,
        "min_water_pct": min_water_pct, "max_water_pct": max_water_pct,
        "min_land_pct": min_land_pct, "max_land_pct": max_land_pct,
        "exclude_offshore": exclude_offshore,
        "exclude_road_corridor": exclude_road_corridor,
        "exclude_rail_corridor": exclude_rail_corridor,
        "exclude_roadlike": exclude_roadlike,
    }
    values = []
    where_sql = build_where(params, values)

    offset = (page - 1) * page_size

    # read from parcel_catalog (cohorts + metrics)
    sql_count = f"SELECT COUNT(*) AS n FROM parcel_catalog {where_sql};"
    sql_page = f"""
        SELECT
          parcel_id, cohort, acres,
          uprn_count,
          uprn_interior_count,
          uprn_boundary_count,
          water_pct, land_pct,
          is_road_corridor, is_rail_corridor, is_offshore
        FROM parcel_catalog
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

    # map to client field names (backwards-compatible)
    items = []
    for r in rows:
        items.append({
            "parcel_id": r["parcel_id"],
            "cohort": r["cohort"],
            "acres": r["acres"],
            "uprn_total": r["uprn_count"],
            "uprn_interior": r["uprn_interior_count"],
            "uprn_boundary": r["uprn_boundary_count"],
            "water_pct": float(r["water_pct"]) if r["water_pct"] is not None else None,
            "land_pct": float(r["land_pct"]) if r["land_pct"] is not None else None,
            "is_road_corridor": r["is_road_corridor"],
            "is_rail_corridor": r["is_rail_corridor"],
            "is_offshore": r["is_offshore"],
        })

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "pages": math.ceil(total / page_size) if page_size else 1,
        "items": items
    }

@app.get("/api/parcels.csv")
def parcels_csv(
    # same filters as list_parcels
    min_acres: float | None = None,
    max_acres: float | None = None,
    min_uprn_interior: int | None = None,
    max_uprn_interior: int | None = None,
    min_uprn_total: int | None = None,
    max_uprn_total: int | None = None,
    min_water_pct: float | None = None,
    max_water_pct: float | None = None,
    min_land_pct: float | None = None,
    max_land_pct: float | None = None,
    exclude_offshore: bool | None = None,
    exclude_road_corridor: bool | None = None,
    exclude_rail_corridor: bool | None = None,
    exclude_roadlike: bool | None = None,
):
    params = {
        "min_acres": min_acres, "max_acres": max_acres,
        "min_uprn_interior": min_uprn_interior, "max_uprn_interior": max_uprn_interior,
        "min_uprn_total": min_uprn_total, "max_uprn_total": max_uprn_total,
        "min_water_pct": min_water_pct, "max_water_pct": max_water_pct,
        "min_land_pct": min_land_pct, "max_land_pct": max_land_pct,
        "exclude_offshore": exclude_offshore,
        "exclude_road_corridor": exclude_road_corridor,
        "exclude_rail_corridor": exclude_rail_corridor,
        "exclude_roadlike": exclude_roadlike,
    }
    values = []
    where_sql = build_where(params, values)

    header = [
        "parcel_id","cohort","acres",
        "uprn_total","uprn_interior","uprn_boundary",
        "water_pct","land_pct",
        "is_road_corridor","is_rail_corridor","is_offshore"
    ]

    def stream_select():
        yield ",".join(header) + "\n"
        chunk = 10000
        with get_conn() as conn:
            with conn.cursor(name="csv_cur") as cur:  # server-side cursor to stream rows
                cur.itersize = chunk
                cur.execute(f"""
                    SELECT
                      parcel_id, cohort, acres,
                      uprn_count AS uprn_total,
                      uprn_interior_count AS uprn_interior,
                      uprn_boundary_count AS uprn_boundary,
                      water_pct, land_pct,
                      is_road_corridor, is_rail_corridor, is_offshore
                    FROM parcel_catalog
                    {where_sql}
                    ORDER BY parcel_id
                """, values)
                for rec in cur:
                    row = [
                        rec["parcel_id"], rec["cohort"], rec["acres"],
                        rec["uprn_total"], rec["uprn_interior"], rec["uprn_boundary"],
                        rec["water_pct"], rec["land_pct"],
                        "true" if rec["is_road_corridor"] else "false",
                        "true" if rec["is_rail_corridor"] else "false",
                        "true" if rec["is_offshore"] else "false",
                    ]
                    yield ",".join(map(str,row)) + "\n"

    return StreamingResponse(stream_select(), media_type="text/csv",
                             headers={"Content-Disposition":"attachment; filename=parcels.csv"})

@app.get("/api/parcels/{parcel_id}/uprns")
def parcel_uprns(parcel_id: int):
    """
    Return UPRNs inside a parcel with an 'interior' boolean and WGS84 coords.
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
      FROM parcel_catalog
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
    return cohort_stats()
