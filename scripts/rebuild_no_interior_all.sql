SET statement_timeout = 0;
SET lock_timeout = '5s';
SET max_parallel_workers_per_gather = 8;
SET work_mem = '256MB';
SET maintenance_work_mem = '512MB';

-- Drop dependents in order
DROP MATERIALIZED VIEW IF EXISTS cohort_parcels_map;
DROP MATERIALIZED VIEW IF EXISTS uprn_catalog;
DROP MATERIALIZED VIEW IF EXISTS parcel_catalog;
DROP MATERIALIZED VIEW IF EXISTS target_cohorts;
DROP MATERIALIZED VIEW IF EXISTS parcel_features;
DROP MATERIALIZED VIEW IF EXISTS parcel_uprn_stats;

-- Safety indexes on base
CREATE INDEX IF NOT EXISTS parcel_uprn_base_pid_ix ON parcel_uprn_base (parcel_id);
CREATE INDEX IF NOT EXISTS parcel_uprn_base_uprn_ix ON parcel_uprn_base (uprn);

-- 1) UPRN stats (no interior)
CREATE MATERIALIZED VIEW parcel_uprn_stats AS
SELECT
  p.parcel_id,
  (p.area_sqm/4046.8564224)::numeric(12,2) AS acres,
  COUNT(b.uprn)::int                        AS uprn_count,
  -- density (total per acre)
  (COUNT(b.uprn)::numeric / NULLIF((p.area_sqm/4046.8564224),0))::numeric(8,4) AS uprn_per_acre
FROM parcel_1acre p
LEFT JOIN parcel_uprn_base b ON b.parcel_id = p.parcel_id
GROUP BY p.parcel_id, p.area_sqm;
CREATE INDEX parcel_uprn_stats_pid_ix ON parcel_uprn_stats (parcel_id);
ANALYZE parcel_uprn_stats;

-- 2) Features (keep cluster count; no interior columns)
CREATE MATERIALIZED VIEW parcel_features AS
SELECT
  s.parcel_id,
  s.acres,
  s.uprn_count,
  COALESCE(c.uprn_cluster_count,0) AS uprn_cluster_count,
  s.uprn_per_acre
FROM parcel_uprn_stats s
LEFT JOIN parcel_uprn_clusters c USING (parcel_id);
CREATE INDEX parcel_features_ix ON parcel_features (parcel_id);
ANALYZE parcel_features;

-- 3) Cohorts (rewritten to use total UPRNs only)
CREATE MATERIALIZED VIEW target_cohorts AS
WITH f AS (
  SELECT
    parcel_id, acres, uprn_count, uprn_per_acre,
    COALESCE(uprn_cluster_count,0) AS uprn_cluster_count
  FROM parcel_features
)
SELECT f.*,
CASE
  WHEN uprn_count = 0 THEN 'A_bare_land'
  -- smallholding: very few UPRNs, small acreage, few clusters
  WHEN acres BETWEEN 1 AND 5
       AND uprn_count BETWEEN 1 AND 3
       AND uprn_cluster_count <= 3
    THEN 'B_single_holding'
  WHEN acres > 5 AND acres <= 25
       AND uprn_count <= 2
       AND uprn_cluster_count <= 2
    THEN 'B_single_holding'
  -- larger sparse holding: extremely low density and <=2 clusters
  WHEN acres > 25
       AND uprn_cluster_count <= 2
       AND uprn_per_acre <= 0.05   -- ~1 per 20 acres
    THEN 'B_single_holding'
  -- dispersed estate: multiple clusters at low-ish density
  WHEN uprn_cluster_count BETWEEN 3 AND 8
       AND uprn_per_acre <= 0.10   -- ~1 per 10 acres
    THEN 'C_dispersed_estate'
  ELSE 'X_exclude'
END AS cohort
FROM f;
CREATE INDEX target_cohorts_cohort_ix ON target_cohorts (cohort);
CREATE INDEX target_cohorts_pid_ix    ON target_cohorts (parcel_id);
ANALYZE target_cohorts;

-- 4) Parcel catalog (cohorts + metrics, no interior/boundary columns)
--    Requires parcel_metrics already built.
CREATE MATERIALIZED VIEW parcel_catalog AS
SELECT
  t.parcel_id,
  t.cohort,
  t.acres,
  t.uprn_count,
  t.uprn_per_acre,
  t.uprn_cluster_count,
  m.water_pct, m.land_pct,
  m.road_ratio, m.rail_ratio,
  m.compactness,
  m.is_offshore, m.is_road_corridor, m.is_rail_corridor, m.is_roadlike_longthin
FROM target_cohorts t
LEFT JOIN parcel_metrics m USING (parcel_id);
CREATE INDEX parcel_catalog_pid_ix    ON parcel_catalog (parcel_id);
CREATE INDEX parcel_catalog_cohort_ix ON parcel_catalog (cohort);
ANALYZE parcel_catalog;

-- 5) Map MV (no gating; show A/B/C only)
CREATE MATERIALIZED VIEW cohort_parcels_map AS
SELECT
  p.parcel_id,
  c.cohort,
  c.acres,
  c.uprn_count,
  c.uprn_per_acre,
  (p.geom_gen)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN parcel_catalog c USING (parcel_id)
WHERE c.cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate');
CREATE INDEX cohort_parcels_map_gix       ON cohort_parcels_map USING GIST (geom);
CREATE INDEX cohort_parcels_map_cohort_ix ON cohort_parcels_map (cohort);
ANALYZE cohort_parcels_map;

-- 6) UPRN catalog (one row per UPRN; no interior flag)
DROP MATERIALIZED VIEW IF EXISTS uprn_catalog;
CREATE MATERIALIZED VIEW uprn_catalog AS
WITH all_hits AS (
  SELECT b.uprn, b.parcel_id, b.ugeom FROM parcel_uprn_base b
),
agg_parcels AS (
  SELECT uprn, array_agg(DISTINCT parcel_id ORDER BY parcel_id) AS parcel_ids
  FROM all_hits GROUP BY uprn
),
primary_pick AS (
  -- simple tie-break: smallest parcel_id if a point is in multiple parcels
  SELECT DISTINCT ON (uprn) uprn, parcel_id
  FROM all_hits
  ORDER BY uprn, parcel_id
),
joined AS (
  SELECT
    p.uprn, p.parcel_id,
    c.cohort, c.acres, c.uprn_count,
    m.water_pct, m.land_pct,
    m.is_road_corridor, m.is_rail_corridor, m.is_offshore
  FROM primary_pick p
  JOIN parcel_catalog  c USING (parcel_id)
  LEFT JOIN parcel_metrics m USING (parcel_id)
)
SELECT
  j.uprn,
  j.parcel_id,       -- primary parcel
  a.parcel_ids,      -- all parcels containing this UPRN
  j.cohort, j.acres, j.uprn_count,
  j.water_pct, j.land_pct,
  j.is_road_corridor, j.is_rail_corridor, j.is_offshore
FROM joined j
JOIN agg_parcels a USING (uprn);
CREATE UNIQUE INDEX uprn_catalog_pk ON uprn_catalog (uprn);
CREATE INDEX uprn_catalog_pid_ix    ON uprn_catalog (parcel_id);
CREATE INDEX uprn_catalog_cohort_ix ON uprn_catalog (cohort);
ANALYZE uprn_catalog;

-- 7) Sanity
SELECT 'parcel_catalog', COUNT(*) FROM parcel_catalog;
SELECT 'uprn_catalog',   COUNT(*) FROM uprn_catalog;
