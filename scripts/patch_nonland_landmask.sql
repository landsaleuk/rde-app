-- Fix land-mask logic: include parcels outside the mask (offshore)
-- and rebuild flags → cohorts → map in the right order.

SET statement_timeout = 0;
SET lock_timeout = '5s';
SET max_parallel_workers_per_gather = 8;
SET work_mem = '256MB';
SET maintenance_work_mem = '512MB';

-- Drop dependents first
DROP MATERIALIZED VIEW IF EXISTS cohort_parcels_map;
DROP MATERIALIZED VIEW IF EXISTS target_cohorts;
DROP MATERIALIZED VIEW IF EXISTS parcel_nonland_flags;

-- Rebuild flags (uses existing roads_buf / rail_buf / gb_land_mask)
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
),
land AS (
  -- *** KEY CHANGE: LEFT JOIN so parcels outside the mask are kept ***
  SELECT b.parcel_id,
         COALESCE(ST_Area(ST_Intersection(b.geom, m.geom)),0)::double precision AS a_land,
         CASE
           WHEN m.geom IS NULL THEN TRUE            -- entirely outside the land mask
           ELSE NOT ST_Covers(m.geom, ST_PointOnSurface(b.geom))
         END AS offshore_centroid
  FROM base b
  LEFT JOIN gb_land_mask m ON b.geom && m.geom
)
SELECT
  b.parcel_id,
  (b.a_parcel/4046.8564224)::numeric(12,2) AS acres,
  (w.a_water / NULLIF(b.a_parcel,0))::numeric(6,4) AS water_ratio,
  (r.a_road  / NULLIF(b.a_parcel,0))::numeric(6,4) AS road_ratio,
  (rl.a_rail / NULLIF(b.a_parcel,0))::numeric(6,4) AS rail_ratio,
  (land.a_land / NULLIF(b.a_parcel,0))::numeric(6,4) AS land_ratio,
  land.offshore_centroid,
  (4*PI()*b.a_parcel / NULLIF( ST_Perimeter(b.geom)::double precision * ST_Perimeter(b.geom)::double precision, 0))::numeric(6,4)
    AS compactness,
  CASE
    WHEN land.offshore_centroid OR (land.a_land / NULLIF(b.a_parcel,0)) < 0.10 THEN TRUE
    WHEN (w.a_water / NULLIF(b.a_parcel,0)) >= 0.50 THEN TRUE
    WHEN (r.a_road  / NULLIF(b.a_parcel,0)) >= 0.60 AND (b.a_parcel/4046.8564224) <= 30 THEN TRUE
    WHEN (rl.a_rail / NULLIF(b.a_parcel,0)) >= 0.50 AND (b.a_parcel/4046.8564224) <= 50 THEN TRUE
    WHEN (b.a_parcel/4046.8564224) <= 20 AND
         (4*PI()*b.a_parcel / NULLIF( ST_Perimeter(b.geom)::double precision * ST_Perimeter(b.geom)::double precision, 0)) < 0.020
      THEN TRUE
    ELSE FALSE
  END AS is_nonland,
  CASE
    WHEN land.offshore_centroid OR (land.a_land / NULLIF(b.a_parcel,0)) < 0.10 THEN 'offshore/land<10%'
    WHEN (w.a_water / NULLIF(b.a_parcel,0)) >= 0.50 THEN 'water>=50%'
    WHEN (r.a_road  / NULLIF(b.a_parcel,0)) >= 0.60 AND (b.a_parcel/4046.8564224) <= 30 THEN 'road corridor'
    WHEN (rl.a_rail / NULLIF(b.a_parcel,0)) >= 0.50 AND (b.a_parcel/4046.8564224) <= 50 THEN 'rail corridor'
    WHEN (b.a_parcel/4046.8564224) <= 20 AND
         (4*PI()*b.a_parcel / NULLIF( ST_Perimeter(b.geom)::double precision * ST_Perimeter(b.geom)::double precision, 0)) < 0.020
      THEN 'long-thin'
    ELSE NULL
  END AS nonland_reason
FROM base b
LEFT JOIN w  ON w.parcel_id  = b.parcel_id
LEFT JOIN r  ON r.parcel_id  = b.parcel_id
LEFT JOIN rl ON rl.parcel_id = b.parcel_id
LEFT JOIN land ON land.parcel_id = b.parcel_id;

CREATE INDEX parcel_nonland_flags_pid_ix ON parcel_nonland_flags (parcel_id);
ANALYZE parcel_nonland_flags;

-- Rebuild cohorts (your current rules) with the non-land gate
CREATE MATERIALIZED VIEW target_cohorts AS
WITH f AS (
  SELECT
    pf.parcel_id, pf.acres, pf.uprn_count, pf.uprn_interior_count,
    pf.uprn_per_acre, COALESCE(pf.uprn_cluster_count,0) AS uprn_cluster_count,
    CASE WHEN pf.uprn_count > 0 THEN pf.uprn_interior_count::float / pf.uprn_count ELSE 0 END AS interior_share
  FROM parcel_features pf
),
nl AS (SELECT parcel_id, is_nonland FROM parcel_nonland_flags)
SELECT f.*,
CASE
  WHEN COALESCE(nl.is_nonland,FALSE)                                   THEN 'X_exclude'
  WHEN uprn_interior_count = 0                                         THEN 'A_bare_land'
  WHEN f.acres BETWEEN 1 AND 5  AND uprn_interior_count BETWEEN 1 AND 3 AND uprn_cluster_count <= 3 AND interior_share >= 0.60
                                                                       THEN 'B_single_holding'
  WHEN f.acres > 5 AND f.acres <= 25 AND uprn_interior_count <= 2 AND uprn_cluster_count <= 2 AND interior_share >= 0.60
                                                                       THEN 'B_single_holding'
  WHEN f.acres > 25 AND uprn_cluster_count <= 2 AND uprn_per_acre <= 0.05
                                                                       THEN 'B_single_holding'
  WHEN uprn_cluster_count BETWEEN 3 AND 8 AND uprn_per_acre <= 0.10    THEN 'C_dispersed_estate'
  ELSE 'X_exclude'
END AS cohort
FROM f LEFT JOIN nl USING (parcel_id);

CREATE INDEX target_cohorts_cohort_ix ON target_cohorts (cohort);
CREATE INDEX target_cohorts_pid_ix    ON target_cohorts (parcel_id);
ANALYZE target_cohorts;

-- Rebuild the map (hard‑gated so it can never show non-land)
SET max_parallel_workers_per_gather = 0;  SET jit = off;

CREATE MATERIALIZED VIEW cohort_parcels_map AS
SELECT
  p.parcel_id,
  t.cohort,
  (p.area_sqm/4046.8564224)::numeric(12,1) AS acres,
  s.uprn_count, s.uprn_interior_count, s.uprn_per_acre,
  (p.geom_gen)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN target_cohorts t USING (parcel_id)
JOIN parcel_nonland_flags nfl USING (parcel_id)   -- hard gate
LEFT JOIN parcel_uprn_stats s USING (parcel_id)
WHERE t.cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate')
  AND NOT nfl.is_nonland;

CREATE INDEX IF NOT EXISTS cohort_parcels_map_gix       ON cohort_parcels_map USING GIST (geom);
CREATE INDEX IF NOT EXISTS cohort_parcels_map_cohort_ix ON cohort_parcels_map (cohort);
ANALYZE cohort_parcels_map;
