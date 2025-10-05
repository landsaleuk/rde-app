-- Fast, one-time derivations from Zoomstack to mark parcels that are not
-- developable land (foreshore/sea/river beds; adopted highways; rail corridors)

SET statement_timeout = 0;
SET max_parallel_workers_per_gather = 8;
SET work_mem = '256MB';
SET maintenance_work_mem = '512MB';

-- 0) Safety: indexes on raw layers
CREATE INDEX IF NOT EXISTS os_water_gix          ON os_water          USING GIST (geom);
CREATE INDEX IF NOT EXISTS os_roads_local_gix    ON os_roads_local    USING GIST (geom);
CREATE INDEX IF NOT EXISTS os_roads_regional_gix ON os_roads_regional USING GIST (geom);
CREATE INDEX IF NOT EXISTS os_roads_national_gix ON os_roads_national USING GIST (geom);
CREATE INDEX IF NOT EXISTS os_rail_gix           ON os_rail           USING GIST (geom);

-- 1) Prebuffer roads & rails once (faster than buffering per parcel)
DROP MATERIALIZED VIEW IF EXISTS roads_buf;
CREATE MATERIALIZED VIEW roads_buf AS
SELECT ST_Buffer(geom, 12.0) AS geom FROM os_roads_national
UNION ALL
SELECT ST_Buffer(geom,  9.0) AS geom FROM os_roads_regional
UNION ALL
SELECT ST_Buffer(geom,  7.0) AS geom FROM os_roads_local;
CREATE INDEX roads_buf_gix ON roads_buf USING GIST (geom);
ANALYZE roads_buf;

DROP MATERIALIZED VIEW IF EXISTS rail_buf;
CREATE MATERIALIZED VIEW rail_buf AS
SELECT ST_Buffer(geom, 7.5) AS geom FROM os_rail;
CREATE INDEX rail_buf_gix ON rail_buf USING GIST (geom);
ANALYZE rail_buf;

-- 2) Compute per-parcel water/road/rail coverage + a "long thin" heuristic
DROP MATERIALIZED VIEW IF EXISTS parcel_nonland_flags;

CREATE MATERIALIZED VIEW parcel_nonland_flags AS
WITH base AS (
  SELECT p.parcel_id, p.geom, p.area_sqm::double precision AS a_parcel
  FROM parcel_1acre p
),
w AS (
  SELECT b.parcel_id,
         COALESCE(SUM(ST_Area(ST_Intersection(b.geom, w.geom))),0)::double precision AS a_water
  FROM base b
  LEFT JOIN os_water w ON b.geom && w.geom AND ST_Intersects(b.geom, w.geom)
  GROUP BY b.parcel_id
),
r AS (
  SELECT b.parcel_id,
         COALESCE(SUM(ST_Area(ST_Intersection(b.geom, rb.geom))),0)::double precision AS a_road
  FROM base b
  LEFT JOIN roads_buf rb ON b.geom && rb.geom AND ST_Intersects(b.geom, rb.geom)
  GROUP BY b.parcel_id
),
rl AS (
  SELECT b.parcel_id,
         COALESCE(SUM(ST_Area(ST_Intersection(b.geom, rlb.geom))),0)::double precision AS a_rail
  FROM base b
  LEFT JOIN rail_buf rlb ON b.geom && rlb.geom AND ST_Intersects(b.geom, rlb.geom)
  GROUP BY b.parcel_id
)
SELECT
  b.parcel_id,
  (b.a_parcel/4046.8564224)::numeric(12,2) AS acres,
  -- ratios (0..1)
  (w.a_water / NULLIF(b.a_parcel,0))::numeric(6,4) AS water_ratio,
  (r.a_road  / NULLIF(b.a_parcel,0))::numeric(6,4) AS road_ratio,
  (rl.a_rail / NULLIF(b.a_parcel,0))::numeric(6,4) AS rail_ratio,
  -- compactness: 1 ~ circle, near 0 ~ long thin strip
  (4*PI()*b.a_parcel / NULLIF( ST_Perimeter(b.geom)::double precision * ST_Perimeter(b.geom)::double precision, 0))::numeric(6,4)
    AS compactness,
  -- classification
  CASE
    WHEN (w.a_water / NULLIF(b.a_parcel,0)) >= 0.50
      THEN TRUE
    WHEN (r.a_road / NULLIF(b.a_parcel,0)) >= 0.60 AND (b.a_parcel/4046.8564224) <= 30
      THEN TRUE
    WHEN (rl.a_rail / NULLIF(b.a_parcel,0)) >= 0.50 AND (b.a_parcel/4046.8564224) <= 50
      THEN TRUE
    WHEN (b.a_parcel/4046.8564224) <= 20 AND
         (4*PI()*b.a_parcel / NULLIF( ST_Perimeter(b.geom)::double precision * ST_Perimeter(b.geom)::double precision, 0)) < 0.020
      THEN TRUE
    ELSE FALSE
  END AS is_nonland,
  CASE
    WHEN (w.a_water / NULLIF(b.a_parcel,0)) >= 0.50 THEN 'water>=50%'
    WHEN (r.a_road  / NULLIF(b.a_parcel,0)) >= 0.60 AND (b.a_parcel/4046.8564224) <= 30 THEN 'road corridor'
    WHEN (rl.a_rail / NULLIF(b.a_parcel,0)) >= 0.50 AND (b.a_parcel/4046.8564224) <= 50 THEN 'rail corridor'
    WHEN (b.a_parcel/4046.8564224) <= 20 AND
         (4*PI()*b.a_parcel / NULLIF( ST_Perimeter(b.geom)::double precision * ST_Perimeter(b.geom)::double precision, 0)) < 0.020
      THEN 'long-thin (likely highway)'
    ELSE NULL
  END AS nonland_reason
FROM base b
LEFT JOIN w  ON w.parcel_id  = b.parcel_id
LEFT JOIN r  ON r.parcel_id  = b.parcel_id
LEFT JOIN rl ON rl.parcel_id = b.parcel_id;

CREATE INDEX parcel_nonland_flags_pid_ix ON parcel_nonland_flags (parcel_id);
ANALYZE parcel_nonland_flags;
