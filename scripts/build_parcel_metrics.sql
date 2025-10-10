SET statement_timeout = 0;
SET lock_timeout = '5s';
SET max_parallel_workers_per_gather = 8;
SET work_mem = '256MB';
SET maintenance_work_mem = '512MB';

DROP MATERIALIZED VIEW IF EXISTS parcel_metrics;

CREATE MATERIALIZED VIEW parcel_metrics AS
WITH base AS (
  SELECT
    p.parcel_id,
    -- Clean, valid, simplified parcel geometry (fast + robust)
    ST_Buffer(ST_MakeValid((p.geom_gen)::geometry(MultiPolygon,27700)), 0) AS geom_ok,
    ST_Area(p.geom_gen)::double precision       AS a_parcel,
    (p.area_sqm/4046.8564224)::numeric(12,2)    AS acres
  FROM parcel_1acre p
),
land AS (
  -- Land overlap via *tiled* land mask (indexed, small pieces)
  SELECT b.parcel_id,
         COALESCE(SUM(ST_Area(ST_Intersection(b.geom_ok, t.geom))),0)::double precision AS a_land
  FROM base b
  JOIN gb_land_mask_tiles t
    ON b.geom_ok && t.geom AND ST_Intersects(b.geom_ok, t.geom)
  GROUP BY b.parcel_id
),
water AS (
  -- Water overlap via *tiled* water mask
  SELECT b.parcel_id,
         COALESCE(SUM(ST_Area(ST_Intersection(b.geom_ok, w.geom))),0)::double precision AS a_water
  FROM base b
  JOIN os_water_tiles w
    ON b.geom_ok && w.geom AND ST_Intersects(b.geom_ok, w.geom)
  GROUP BY b.parcel_id
),
r AS (
  -- Road corridor overlap
  SELECT b.parcel_id,
         COALESCE(SUM(ST_Area(ST_Intersection(b.geom_ok, rb.geom))),0)::double precision AS a_road
  FROM base b
  LEFT JOIN roads_buf rb
    ON b.geom_ok && rb.geom AND ST_Intersects(b.geom_ok, rb.geom)
  GROUP BY b.parcel_id
),
rl AS (
  -- Rail corridor overlap
  SELECT b.parcel_id,
         COALESCE(SUM(ST_Area(ST_Intersection(b.geom_ok, rlb.geom))),0)::double precision AS a_rail
  FROM base b
  LEFT JOIN rail_buf rlb
    ON b.geom_ok && rlb.geom AND ST_Intersects(b.geom_ok, rlb.geom)
  GROUP BY b.parcel_id
),
centroid AS (
  -- Offshore if centroid not covered by ANY land tile
  SELECT b.parcel_id,
         CASE
           WHEN EXISTS (
             SELECT 1
             FROM gb_land_mask_tiles t
             WHERE t.geom && ST_Expand(ST_PointOnSurface(b.geom_ok), 1)
               AND ST_Covers(t.geom, ST_PointOnSurface(b.geom_ok))
           ) THEN FALSE ELSE TRUE
         END AS is_offshore
  FROM base b
)
SELECT
  b.parcel_id,
  b.acres,
  -- ratios (0..1)
  (land.a_land  / NULLIF(b.a_parcel,0))::numeric(6,4) AS land_ratio,
  (water.a_water/ NULLIF(b.a_parcel,0))::numeric(6,4) AS water_ratio,
  (r.a_road     / NULLIF(b.a_parcel,0))::numeric(6,4) AS road_ratio,
  (rl.a_rail    / NULLIF(b.a_parcel,0))::numeric(6,4) AS rail_ratio,

  -- 1-decimal percentages for UI filters
  ROUND(100 * (water.a_water / NULLIF(b.a_parcel,0))::numeric, 1) AS water_pct,
  ROUND(100 * (land.a_land  / NULLIF(b.a_parcel,0))::numeric, 1)  AS land_pct,

  -- shape heuristic (compactness) computed on the cleaned geometry
  (4*PI()*b.a_parcel / NULLIF(
       ST_Perimeter(b.geom_ok)::double precision * ST_Perimeter(b.geom_ok)::double precision, 0)
  )::numeric(6,4) AS compactness,

  -- booleans you asked for
  centroid.is_offshore,
  (r.a_road  / NULLIF(b.a_parcel,0)) >= 0.65 AND b.acres <= 40 AS is_road_corridor,
  (rl.a_rail / NULLIF(b.a_parcel,0)) >= 0.50 AND b.acres <= 60 AS is_rail_corridor,
  (b.acres <= 30 AND
   (4*PI()*b.a_parcel / NULLIF(
       ST_Perimeter(b.geom_ok)::double precision * ST_Perimeter(b.geom_ok)::double precision, 0)
  ) < 0.030) AS is_roadlike_longthin

FROM base b
LEFT JOIN land     ON land.parcel_id     = b.parcel_id
LEFT JOIN water    ON water.parcel_id    = b.parcel_id
LEFT JOIN r        ON r.parcel_id        = b.parcel_id
LEFT JOIN rl       ON rl.parcel_id       = b.parcel_id
LEFT JOIN centroid ON centroid.parcel_id = b.parcel_id;

CREATE INDEX parcel_metrics_pid_ix ON parcel_metrics (parcel_id);
ANALYZE parcel_metrics;