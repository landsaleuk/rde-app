SET statement_timeout = 0;
SET lock_timeout = '5s';
SET max_parallel_workers_per_gather = 8;

-- Drop dependents
DROP MATERIALIZED VIEW IF EXISTS cohort_parcels_map;
DROP MATERIALIZED VIEW IF EXISTS target_cohorts;
DROP MATERIALIZED VIEW IF EXISTS nfl_gate;
DROP MATERIALIZED VIEW IF EXISTS parcel_nonland_flags;
DROP MATERIALIZED VIEW IF EXISTS roads_buf;
DROP MATERIALIZED VIEW IF EXISTS rail_buf;

-- Corridor buffers (wider than before)
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

-- Flags (NO water; use geom_gen to speed geometry ops)
CREATE MATERIALIZED VIEW parcel_nonland_flags AS
WITH base AS (
  SELECT
    p.parcel_id,
    (p.geom_gen)::geometry(MultiPolygon,27700) AS geom,   -- << simplified
    ST_Area(p.geom_gen)::double precision       AS a_parcel,
    (p.area_sqm/4046.8564224)::numeric(12,2)    AS acres
  FROM parcel_1acre p
),
land AS (
  -- CROSS JOIN to single-row mask => exactly one row per parcel_id
  SELECT b.parcel_id,
         COALESCE(ST_Area(ST_Intersection(b.geom, m.geom)),0)::double precision AS a_land,
         NOT ST_Covers(m.geom, ST_PointOnSurface(b.geom)) AS offshore_centroid
  FROM base b
  CROSS JOIN gb_land_mask m
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
  b.acres,
  (land.a_land / NULLIF(b.a_parcel,0))::numeric(6,4) AS land_ratio,
  (r.a_road  / NULLIF(b.a_parcel,0))::numeric(6,4)   AS road_ratio,
  (rl.a_rail / NULLIF(b.a_parcel,0))::numeric(6,4)   AS rail_ratio,
  -- compactness from simplified geom (fast, good enough)
  (4*PI()*b.a_parcel / NULLIF( ST_Perimeter(b.geom)::double precision * ST_Perimeter(b.geom)::double precision, 0))::numeric(6,4)
    AS compactness,
  CASE
    WHEN land.offshore_centroid OR (land.a_land / NULLIF(b.a_parcel,0)) < 0.05 THEN TRUE
    WHEN (r.a_road  / NULLIF(b.a_parcel,0)) >= 0.65 AND b.acres <= 40 THEN TRUE
    WHEN (rl.a_rail / NULLIF(b.a_parcel,0)) >= 0.50 AND b.acres <= 60 THEN TRUE
    WHEN b.acres <= 30 AND
         (4*PI()*b.a_parcel / NULLIF( ST_Perimeter(b.geom)::double precision * ST_Perimeter(b.geom)::double precision, 0)) < 0.030
      THEN TRUE
    ELSE FALSE
  END AS is_nonland,
  CASE
    WHEN land.offshore_centroid OR (land.a_land / NULLIF(b.a_parcel,0)) < 0.05 THEN 'offshore/land<5%'
    WHEN (r.a_road  / NULLIF(b.a_parcel,0)) >= 0.65 AND b.acres <= 40 THEN 'road corridor'
    WHEN (rl.a_rail / NULLIF(b.a_parcel,0)) >= 0.50 AND b.acres <= 60 THEN 'rail corridor'
    WHEN b.acres <= 30 AND
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

-- One row per parcel (TRUE wins)
CREATE MATERIALIZED VIEW nfl_gate AS
SELECT parcel_id, BOOL_OR(is_nonland) AS is_nonland
FROM parcel_nonland_flags
GROUP BY parcel_id;
CREATE INDEX nfl_gate_pid_ix ON nfl_gate(parcel_id);
ANALYZE nfl_gate;

-- Cohorts (your rules) gated by nfl_gate
CREATE MATERIALIZED VIEW target_cohorts AS
WITH f AS (
  SELECT
    pf.parcel_id,
    pf.acres,
    pf.uprn_count,
    pf.uprn_interior_count,
    pf.uprn_per_acre,
    COALESCE(pf.uprn_cluster_count,0) AS uprn_cluster_count,
    CASE WHEN pf.uprn_count > 0 THEN pf.uprn_interior_count::float / pf.uprn_count ELSE 0 END AS interior_share
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

-- Map (hard-gated)
CREATE MATERIALIZED VIEW cohort_parcels_map AS
SELECT
  p.parcel_id,
  t.cohort,
  (p.area_sqm/4046.8564224)::numeric(12,1) AS acres,
  s.uprn_count, s.uprn_interior_count, s.uprn_per_acre,
  (p.geom_gen)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN target_cohorts t USING (parcel_id)
JOIN nfl_gate ng USING (parcel_id)
LEFT JOIN parcel_uprn_stats s USING (parcel_id)
WHERE t.cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate')
  AND NOT ng.is_nonland;

CREATE INDEX cohort_parcels_map_gix       ON cohort_parcels_map USING GIST (geom);
CREATE INDEX cohort_parcels_map_cohort_ix ON cohort_parcels_map (cohort);
ANALYZE cohort_parcels_map;
