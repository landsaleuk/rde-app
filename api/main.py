import os, math
import psycopg2, psycopg2.extras
from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.templating import Jinja2Templates
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

# -------------------- PARCEL LIST --------------------

def build_where_parcels(params, values):
    where = ["cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate')"]

    # acres
    if params.get("min_acres") is not None:
        where.append("acres >= %s"); values.append(params["min_acres"])
    if params.get("max_acres") is not None:
        where.append("acres <= %s"); values.append(params["max_acres"])

    # total UPRNs
    if params.get("min_uprn_total") is not None:
        where.append("uprn_count >= %s"); values.append(params["min_uprn_total"])
    if params.get("max_uprn_total") is not None:
        where.append("uprn_count <= %s"); values.append(params["max_uprn_total"])

    # metrics
    if params.get("min_water_pct") is not None:
        where.append("water_pct >= %s"); values.append(params["min_water_pct"])
    if params.get("max_water_pct") is not None:
        where.append("water_pct <= %s"); values.append(params["max_water_pct"])
    if params.get("min_land_pct") is not None:
        where.append("land_pct >= %s"); values.append(params["min_land_pct"])
    if params.get("max_land_pct") is not None:
        where.append("land_pct <= %s"); values.append(params["max_land_pct"])

    # boolean excludes
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
    # filters
    min_acres: float | None = None,
    max_acres: float | None = None,
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
    # sorting
    sort_by: str = Query("parcel_id"),
    sort_dir: str = Query("asc")
):
    allowed_sorts = {"parcel_id","cohort","acres","uprn_count","water_pct","land_pct"}
    if sort_by not in allowed_sorts:
        sort_by = "parcel_id"
    sort_dir = "desc" if sort_dir.lower() == "desc" else "asc"

    params = {
        "min_acres": min_acres, "max_acres": max_acres,
        "min_uprn_total": min_uprn_total, "max_uprn_total": max_uprn_total,
        "min_water_pct": min_water_pct, "max_water_pct": max_water_pct,
        "min_land_pct": min_land_pct, "max_land_pct": max_land_pct,
        "exclude_offshore": exclude_offshore,
        "exclude_road_corridor": exclude_road_corridor,
        "exclude_rail_corridor": exclude_rail_corridor,
        "exclude_roadlike": exclude_roadlike,
    }
    values = []
    where_sql = build_where_parcels(params, values)
    offset = (page - 1) * page_size

    sql_count = f"SELECT COUNT(*) AS n FROM parcel_catalog {where_sql};"
    sql_page = f"""
        SELECT parcel_id, cohort, acres,
               uprn_count,
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

    items = []
    for r in rows:
        items.append({
            "parcel_id": r["parcel_id"],
            "cohort": r["cohort"],
            "acres": r["acres"],
            "uprn_total": r["uprn_count"],
            "water_pct": float(r["water_pct"]) if r["water_pct"] is not None else None,
            "land_pct": float(r["land_pct"]) if r["land_pct"] is not None else None,
            "is_road_corridor": r["is_road_corridor"],
            "is_rail_corridor": r["is_rail_corridor"],
            "is_offshore": r["is_offshore"],
        })

    return {"page": page, "page_size": page_size, "total": total,
            "pages": math.ceil(total / page_size) if page_size else 1,
            "items": items}

@app.get("/api/parcels.csv")
def parcels_csv(
    min_acres: float | None = None,
    max_acres: float | None = None,
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
        "min_uprn_total": min_uprn_total, "max_uprn_total": max_uprn_total,
        "min_water_pct": min_water_pct, "max_water_pct": max_water_pct,
        "min_land_pct": min_land_pct, "max_land_pct": max_land_pct,
        "exclude_offshore": exclude_offshore,
        "exclude_road_corridor": exclude_road_corridor,
        "exclude_rail_corridor": exclude_rail_corridor,
        "exclude_roadlike": exclude_roadlike,
    }
    values = []
    where_sql = build_where_parcels(params, values)

    header = ["parcel_id","cohort","acres","uprn_total",
              "water_pct","land_pct","is_road_corridor","is_rail_corridor","is_offshore"]

    def stream_select():
        yield ",".join(header) + "\n"
        chunk = 10000
        with get_conn() as conn:
            with conn.cursor(name="csv_cur") as cur:
                cur.itersize = chunk
                cur.execute(f"""
                    SELECT parcel_id, cohort, acres,
                           uprn_count AS uprn_total,
                           water_pct, land_pct,
                           is_road_corridor, is_rail_corridor, is_offshore
                    FROM parcel_catalog
                    {where_sql}
                    ORDER BY parcel_id
                """, values)
                for rec in cur:
                    row = [
                        rec["parcel_id"], rec["cohort"], rec["acres"],
                        rec["uprn_total"],
                        rec["water_pct"], rec["land_pct"],
                        "true" if rec["is_road_corridor"] else "false",
                        "true" if rec["is_rail_corridor"] else "false",
                        "true" if rec["is_offshore"] else "false",
                    ]
                    yield ",".join(map(str,row)) + "\n"

    return StreamingResponse(stream_select(), media_type="text/csv",
                             headers={"Content-Disposition":"attachment; filename=parcels.csv"})

# Return UPRNs for a parcel (no interior flag)
@app.get("/api/parcels/{parcel_id}/uprns")
def parcel_uprns(parcel_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
              SELECT b.uprn,
                     ST_X(ST_Transform(b.ugeom,4326)) AS lon,
                     ST_Y(ST_Transform(b.ugeom,4326)) AS lat
              FROM parcel_uprn_base b
              WHERE b.parcel_id = %s
              ORDER BY b.uprn;
            """, (parcel_id,))
            rows = cur.fetchall()
            return {"parcel_id": parcel_id, "uprns": rows}

# basic cohort counts for header widgets
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

# -------------------- UPRN LIST --------------------

@app.get("/uprns")
def uprns_page(request: Request):
    return templates.TemplateResponse("uprns.html", {"request": request})

def build_where_uprn(params, values):
    where = ["cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate')"]

    if params.get("min_acres") is not None:
        where.append("acres >= %s"); values.append(params["min_acres"])
    if params.get("max_acres") is not None:
        where.append("acres <= %s"); values.append(params["max_acres"])

    if params.get("min_water_pct") is not None:
        where.append("water_pct >= %s"); values.append(params["min_water_pct"])
    if params.get("max_water_pct") is not None:
        where.append("water_pct <= %s"); values.append(params["max_water_pct"])
    if params.get("min_land_pct") is not None:
        where.append("land_pct >= %s"); values.append(params["min_land_pct"])
    if params.get("max_land_pct") is not None:
        where.append("land_pct <= %s"); values.append(params["max_land_pct"])

    return "WHERE " + " AND ".join(where)

@app.get("/api/uprns")
def list_uprns(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    min_acres: float | None = None,
    max_acres: float | None = None,
    min_water_pct: float | None = None,
    max_water_pct: float | None = None,
    min_land_pct: float | None = None,
    max_land_pct: float | None = None,
    sort_by: str = Query("uprn"),
    sort_dir: str = Query("asc")
):
    allowed_sorts = {"uprn","parcel_id","cohort","acres","uprn_count","water_pct","land_pct"}
    if sort_by not in allowed_sorts:
        sort_by = "uprn"
    sort_dir = "desc" if sort_dir.lower() == "desc" else "asc"

    params = {
        "min_acres": min_acres, "max_acres": max_acres,
        "min_water_pct": min_water_pct, "max_water_pct": max_water_pct,
        "min_land_pct": min_land_pct, "max_land_pct": max_land_pct,
    }
    values = []
    where_sql = build_where_uprn(params, values)
    offset = (page - 1) * page_size

    sql_count = f"SELECT COUNT(*) AS n FROM uprn_catalog {where_sql};"
    sql_page = f"""
        SELECT uprn, parcel_id, parcel_ids, cohort, acres,
               uprn_count, water_pct, land_pct,
               is_road_corridor, is_rail_corridor, is_offshore
        FROM uprn_catalog
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

    return {"page": page, "page_size": page_size, "total": total,
            "pages": math.ceil(total / page_size) if page_size else 1, "items": rows}

@app.get("/api/uprns.csv")
def uprns_csv(
    min_acres: float | None = None,
    max_acres: float | None = None,
    min_water_pct: float | None = None,
    max_water_pct: float | None = None,
    min_land_pct: float | None = None,
    max_land_pct: float | None = None,
):
    params = {
        "min_acres": min_acres, "max_acres": max_acres,
        "min_water_pct": min_water_pct, "max_water_pct": max_water_pct,
        "min_land_pct": min_land_pct, "max_land_pct": max_land_pct,
    }
    values = []
    where_sql = build_where_uprn(params, values)

    header = ["uprn","parcel_id","parcel_ids","cohort","acres","uprn_count",
              "water_pct","land_pct","is_road_corridor","is_rail_corridor","is_offshore"]

    def stream_select():
        yield ",".join(header) + "\n"
        chunk = 10000
        with get_conn() as conn:
            with conn.cursor(name="csv_cur") as cur:
                cur.itersize = chunk
                cur.execute(f"""
                  SELECT uprn, parcel_id, parcel_ids, cohort, acres, uprn_count,
                         water_pct, land_pct, is_road_corridor, is_rail_corridor, is_offshore
                  FROM uprn_catalog
                  {where_sql}
                  ORDER BY uprn
                """, values)
                for rec in cur:
                    row = [
                        rec["uprn"], rec["parcel_id"],
                        "{" + ",".join(map(str, rec["parcel_ids"])) + "}",
                        rec["cohort"], rec["acres"], rec["uprn_count"],
                        rec["water_pct"], rec["land_pct"],
                        "true" if rec["is_road_corridor"] else "false",
                        "true" if rec["is_rail_corridor"] else "false",
                        "true" if rec["is_offshore"] else "false",
                    ]
                    yield ",".join(map(str,row)) + "\n"

    return StreamingResponse(stream_select(), media_type="text/csv",
                             headers={"Content-Disposition":"attachment; filename=uprns.csv"})

# small parcel detail for UPRN click-through (no interior/boundary)
@app.get("/api/parcels/{parcel_id}")
def parcel_detail(parcel_id: int):
    sql = """
      SELECT parcel_id, cohort, acres,
             uprn_count,
             water_pct, land_pct,
             is_road_corridor, is_rail_corridor, is_offshore
      FROM parcel_catalog
      WHERE parcel_id = %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (parcel_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Parcel not found")
            row["water_pct"] = float(row["water_pct"]) if row["water_pct"] is not None else None
            row["land_pct"]  = float(row["land_pct"])  if row["land_pct"]  is not None else None
            return row
