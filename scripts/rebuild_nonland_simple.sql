-- =====================================================================
-- Simple, robust non-land detection:
--  * Land mask from os_land with seam-closing (buffer out, then in)
--  * Road / Rail corridors with wider buffers
--  * Long-thin "road-like" heuristic
--  * NO water polygons used
--  * Aggregated gate (one row per parcel) to avoid duplicate flag issues
--  * Rebuild cohorts + map (hard-gated)
-- =====================================================================

SET statement_timeout = 0;
SET lock_timeout = '5s';
SET max_parallel_workers_per_gather = 8;
SET work_mem = '256MB';
SET maintenance_work_mem = '512MB';

-- ---- 0) Drop dependents so we can rebuild cleanly --------------------
DROP MATERIALIZED VIEW IF EXISTS cohort_parcels_map;
DROP MATERIALIZED VIEW IF EXISTS target_cohorts;
DROP MATERIALIZED VIEW IF EXISTS nonland_parcels_map;
DROP MATERIALIZED VIEW IF EXISTS nfl_gate;
DROP MATERIALIZED VIEW IF EXISTS parcel_nonland_flags;
DROP MATERIALIZED VIEW IF EXISTS rail_buf;
DROP MATERIALIZED VIEW IF EXISTS roads_buf;
DROP MATERIALIZED VIEW IF EXISTS gb_land_mask;

-- ---- 1) Land mask with seam-closing ---------------------------------
-- Close tiny gaps between os_land tiles: buffer OUT by eps, union, buffer IN by eps.
-- Tune eps if you still see seam artifacts: 8.0m is a good starting point (try 12.0 if needed).
CREATE MATERIALIZED VIEW gb_land_mask AS
SELECT
  ST_CollectionExtract(               -- ensure MultiPolygon output
    ST_Buffer(
      ST_UnaryUnion(
        ST_Buffer(geom, 8.0)          -- grow a bit to "weld" tile edges
      ),
      -8.0                             -- shrink back to coastline
    ), 3) AS geom
FROM os_land;

CREATE INDEX gb_land_mask_gix ON gb_land_mask USING GIST (geom);
ANALYZE gb_land_mask;

-- ---- 2) Corridor buffers (wider than before) ------------------------
CREATE MATERIALIZED VIEW roads_buf AS
SELECT ST_Buffer(geom, 15.0) AS geom FROM os_roads_national
UNION ALL
SELECT ST_Buffer(geom, 12.0) AS geom FROM os_roads_regional
UNION ALL
SELECT ST_Buffer(geom,  9.0) AS geom FROM os_roads_local;
CREATE INDEX roads_buf_gix ON roads_buf USING GIST (geom);
ANALYZE roads_buf;

CREATE MATERIALIZED VIEW rail_buf AS
SELECT ST_Buffer(geom, 10.0) AS geom FROM os_rail;
CREATE INDEX rail_buf_gix ON rail_buf USING GIST (geom);
ANALYZE rail_buf;

-- ---- 3) Parcel-level flags (NO water; land + road/rail + long-thin) --
CREATE MATERIALIZED VIEW parcel_nonland_flags AS
WITH base AS (
  SELECT p.parcel_id, p.geom, p.area_sqm::double precision AS a_parcel
  FROM parcel_1acre p
),
-- Land overlap using the (now continuous) mask; CROSS JOIN to single-row mask
land AS (
  SELECT b.parcel_id,
         COALESCE(ST_Area(ST_Intersection(b.geom, m.geom)),0)::double precision AS a_land,
         CASE
           WHEN m.geom IS NULL THEN TRUE
           ELSE NOT ST_Covers(m.geom, ST_PointOnSurface(b.geom))
         END AS offshore_centroid
  FROM base b
  CROSS JOIN gb_land_mask m
),
-- Corridor overlaps
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
  (land.a_land / NULLIF(b.a_parcel,0))::numeric(6,4) AS land_ratio,
  (r.a_road  / NULLIF(b.a_parcel,0))::numeric(6,4) AS road_ratio,
  (rl.a_rail / NULLIF(b.a_parcel,0))::numeric(6,4) AS rail_ratio,
  -- compactness: 1 = circle, ~0 = long thin
  (4*PI()*b.a_parcel / NULLIF( ST_Perimeter(b.geom)::double precision * ST_Perimeter(b.geom)::double precision, 0))::numeric(6,4)
    AS compactness,
  -- classification (conservative to reduce false positives)
  CASE
    WHEN land.offshore_centroid OR (land.a_land / NULLIF(b.a_parcel,0)) < 0.05 THEN TRUE           -- offshore or <5% on land
    WHEN (r.a_road  / NULLIF(b.a_parcel,0)) >= 0.65 AND (b.a_parcel/4046.8564224) <= 40 THEN TRUE  -- road corridor (wider + up to 40 acres)
    WHEN (rl.a_rail / NULLIF(b.a_parcel,0)) >= 0.50 AND (b.a_parcel/4046.8564224) <= 60 THEN TRUE  -- rail corridor
    WHEN (b.a_parcel/4046.8564224) <= 30 AND
         (4*PI()*b.a_parcel / NULLIF( ST_Perimeter(b.geom)::double precision * ST_Perimeter(b.geom)::double precision, 0)) < 0.030
      THEN TRUE                                                                                    -- long-thin highway-like
    ELSE FALSE
  END AS is_nonland,
  CASE
    WHEN land.offshore_centroid OR (land.a_land / NULLIF(b.a_parcel,0)) < 0.05 THEN 'offshore/land<5%'
    WHEN (r.a_road  / NULLIF(b.a_parcel,0)) >= 0.65 AND (b.a_parcel/4046.8564224) <= 40 THEN 'road corridor'
    WHEN (rl.a_rail / NULLIF(b.a_parcel,0)) >= 0.50 AND (b.a_parcel/4046.8564224) <= 60 THEN 'rail corridor'
    WHEN (b.a_parcel/4046.8564224) <= 30 AND
         (4*PI()*b.a_parcel / NULLIF( ST_Perimeter(b.geom)::double precision * ST_Perimeter(b.geom)::double precision, 0)) < 0.030
      THEN 'long-thin'
    ELSE NULL
  END AS nonland_reason
FROM base b
LEFT JOIN land ON land.parcel_id = b.parcel_id
LEFT JOIN r    ON r.parcel_id    = b.parcel_id
LEFT JOIN rl   ON rl.parcel_id   = b.parcel_id;

CREATE INDEX parcel_nonland_flags_pid_ix ON parcel_nonland_flags (parcel_id);
ANALYZE parcel_nonland_flags;

-- ---- 4) Aggregate gate: exactly one row per parcel -------------------
CREATE MATERIALIZED VIEW nfl_gate AS
SELECT parcel_id,
       BOOL_OR(is_nonland) AS is_nonland
FROM parcel_nonland_flags
GROUP BY parcel_id;

CREATE INDEX nfl_gate_pid_ix ON nfl_gate(parcel_id);
ANALYZE nfl_gate;

-- ---- 5) Rebuild cohorts (your current rules) gated by nfl_gate -------
CREATE MATERIALIZED VIEW target_cohorts AS
WITH f AS (
  SELECT
    pf.parcel_id,
    pf.acres,
    pf.uprn_count,
    pf.uprn_interior_count,
    pf.uprn_per_acre,
    COALESCE(pf.uprn_cluster_count,0) AS uprn_cluster_count,
    CASE WHEN pf.uprn_count > 0
         THEN pf.uprn_interior_count::float / pf.uprn_count
         ELSE 0 END AS interior_share
  FROM parcel_features pf
)
SELECT f.*,
CASE
  WHEN ng.is_nonland                                         THEN 'X_exclude'
  WHEN uprn_interior_count = 0                               THEN 'A_bare_land'
  WHEN f.acres BETWEEN 1 AND 5  AND uprn_interior_count BETWEEN 1 AND 3
       AND uprn_cluster_count <= 3 AND interior_share >= 0.60 THEN 'B_single_holding'
  WHEN f.acres > 5 AND f.acres <= 25 AND uprn_interior_count <= 2
       AND uprn_cluster_count <= 2 AND interior_share >= 0.60 THEN 'B_single_holding'
  WHEN f.acres > 25 AND uprn_cluster_count <= 2 AND uprn_per_acre <= 0.05
                                                             THEN 'B_single_holding'
  WHEN uprn_cluster_count BETWEEN 3 AND 8 AND uprn_per_acre <= 0.10
                                                             THEN 'C_dispersed_estate'
  ELSE 'X_exclude'
END AS cohort
FROM f
JOIN nfl_gate ng USING (parcel_id);

CREATE INDEX target_cohorts_cohort_ix ON target_cohorts (cohort);
CREATE INDEX target_cohorts_pid_ix    ON target_cohorts (parcel_id);
ANALYZE target_cohorts;

-- ---- 6) Map (hard-gated via nfl_gate) --------------------------------
CREATE MATERIALIZED VIEW cohort_parcels_map AS
SELECT
  p.parcel_id,
  t.cohort,
  (p.area_sqm/4046.8564224)::numeric(12,1) AS acres,
  s.uprn_count, s.uprn_interior_count, s.uprn_per_acre,
  (p.geom_gen)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN target_cohorts t USING (parcel_id)
JOIN nfl_gate ng USING (parcel_id)         -- hard gate
LEFT JOIN parcel_uprn_stats s USING (parcel_id)
WHERE t.cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate')
  AND NOT ng.is_nonland;

CREATE INDEX cohort_parcels_map_gix       ON cohort_parcels_map USING GIST (geom);
CREATE INDEX cohort_parcels_map_cohort_ix ON cohort_parcels_map (cohort);
ANALYZE cohort_parcels_map;

-- ---- 7) Optional: QA layer to see what's excluded --------------------
CREATE MATERIALIZED VIEW nonland_parcels_map AS
SELECT
  p.parcel_id,
  (p.area_sqm/4046.8564224)::numeric(12,2) AS acres,
  f.land_ratio, f.road_ratio, f.rail_ratio, f.compactness,
  f.nonland_reason,
  (p.geom_gen)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN parcel_nonland_flags f USING (parcel_id)
WHERE f.is_nonland;

CREATE INDEX nonland_parcels_map_gix ON nonland_parcels_map USING GIST (geom);
ANALYZE nonland_parcels_map;
