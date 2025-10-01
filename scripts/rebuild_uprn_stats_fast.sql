-- =====================================================================
-- Fast rebuild of UPRN→parcel stats & cohorts (idempotent)
-- =====================================================================

SET statement_timeout = 0;
SET max_parallel_workers_per_gather = 8;
SET parallel_leader_participation   = off;
SET work_mem                        = '256MB';
SET maintenance_work_mem            = '512MB';
CREATE EXTENSION IF NOT EXISTS postgis;

-- Drop dependents first, then old base/tables
DROP MATERIALIZED VIEW IF EXISTS target_cohorts;
DROP MATERIALIZED VIEW IF EXISTS parcel_features;
DROP MATERIALIZED VIEW IF EXISTS uprn_interior_counts;
DROP MATERIALIZED VIEW IF EXISTS uprn_counts;
DROP MATERIALIZED VIEW IF EXISTS parcel_interior;

DROP TABLE IF EXISTS parcel_uprn_clusters;
DROP TABLE IF EXISTS parcel_uprn_stats;
DROP TABLE IF EXISTS parcel_uprn_base;

-- Guardrails: fail with a clear message if sources are missing
DO $$
BEGIN
  IF to_regclass('public.parcel_1acre') IS NULL THEN
    RAISE EXCEPTION 'parcel_1acre is missing. Rebuild it before running this script.';
  END IF;
  IF to_regclass('public.os_open_uprn') IS NULL THEN
    RAISE EXCEPTION 'os_open_uprn is missing. Import UPRNs before running this script.';
  END IF;
END$$;

-- Source indexes (safe no-ops if already there)
CREATE INDEX IF NOT EXISTS parcel_1acre_geom_gix ON parcel_1acre USING GIST (geom);
CREATE INDEX IF NOT EXISTS os_open_uprn_gix       ON os_open_uprn USING GIST (geom);

-- 1) Base: every UPRN covered by each parcel (boundary points count as inside)
CREATE TABLE parcel_uprn_base AS
SELECT p.parcel_id, p.geom AS pgeom, p.area_sqm, u.uprn, u.geom AS ugeom
FROM   parcel_1acre p
JOIN   os_open_uprn u
  ON   ST_Covers(p.geom, u.geom);

CREATE INDEX pub_parcel_ix ON parcel_uprn_base (parcel_id);
CREATE INDEX pub_uprn_ix   ON parcel_uprn_base (uprn);
CREATE INDEX pub_pgeom_gix ON parcel_uprn_base USING GIST (pgeom);
CREATE INDEX pub_ugeom_gix ON parcel_uprn_base USING GIST (ugeom);
ANALYZE parcel_uprn_base;

-- 2) Fast outer counts (no geometry)
CREATE MATERIALIZED VIEW uprn_counts AS
SELECT parcel_id, COUNT(*)::int AS uprn_count
FROM   parcel_uprn_base
GROUP  BY parcel_id;
CREATE INDEX ON uprn_counts(parcel_id);

-- 3) Precompute interior shapes once (5 m shrink, simplified)
CREATE MATERIALIZED VIEW parcel_interior AS
SELECT parcel_id,
       ST_SimplifyPreserveTopology(ST_Buffer(geom, -5), 1.0) AS i_geom,
       area_sqm
FROM   parcel_1acre;
CREATE INDEX ON parcel_interior(parcel_id);
CREATE INDEX parcel_interior_gix ON parcel_interior USING GIST (i_geom);

-- 4) Interior counts with bbox prefilter then exact ST_Covers
CREATE MATERIALIZED VIEW uprn_interior_counts AS
SELECT b.parcel_id, COUNT(*)::int AS uprn_interior_count
FROM   parcel_uprn_base b
JOIN   parcel_interior i ON i.parcel_id = b.parcel_id
WHERE  b.ugeom && i.i_geom
  AND  ST_Covers(i.i_geom, b.ugeom)
GROUP  BY b.parcel_id;
CREATE INDEX ON uprn_interior_counts(parcel_id);

-- 5) Assemble stats
CREATE TABLE parcel_uprn_stats AS
SELECT p.parcel_id,
       (p.area_sqm/4046.8564224)::numeric(12,2)               AS acres,
       COALESCE(c.uprn_count,0)                               AS uprn_count,
       COALESCE(ic.uprn_interior_count,0)                     AS uprn_interior_count,
       (COALESCE(c.uprn_count,0)/NULLIF(p.area_sqm/4046.8564224,0))::numeric(10,4)
         AS uprn_per_acre
FROM   parcel_1acre p
LEFT   JOIN uprn_counts          c  USING (parcel_id)
LEFT   JOIN uprn_interior_counts ic USING (parcel_id);
CREATE INDEX parcel_uprn_stats_ix ON parcel_uprn_stats(parcel_id);
ANALYZE parcel_uprn_stats;

-- 6) (Optional) cluster counts (commented for speed)
DROP TABLE IF EXISTS parcel_uprn_clusters;
CREATE TABLE parcel_uprn_clusters AS
SELECT parcel_id, COUNT(DISTINCT cluster_id)::int AS uprn_cluster_count
FROM (
  SELECT parcel_id,
         ST_ClusterDBSCAN(ugeom, eps := 75, minpoints := 1)
           OVER (PARTITION BY parcel_id) AS cluster_id
  FROM parcel_uprn_base
) s
GROUP BY parcel_id;
CREATE INDEX parcel_uprn_clusters_ix ON parcel_uprn_clusters (parcel_id);
ANALYZE parcel_uprn_clusters;

-- 7) Features + cohorts (assume 0 clusters if you skipped step 6)
CREATE MATERIALIZED VIEW parcel_features AS
SELECT s.parcel_id, s.acres, s.uprn_count, s.uprn_interior_count,
       COALESCE(c.uprn_cluster_count,0) AS uprn_cluster_count,
       s.uprn_per_acre
FROM   parcel_uprn_stats s
LEFT   JOIN parcel_uprn_clusters c USING (parcel_id);
CREATE INDEX parcel_features_ix ON parcel_features (parcel_id);

CREATE MATERIALIZED VIEW target_cohorts AS
SELECT f.*,
CASE
  WHEN uprn_interior_count = 0 THEN 'A_bare_land'
  WHEN uprn_cluster_count <= 2 AND uprn_per_acre <= 0.05 THEN 'B_single_holding'
  WHEN uprn_cluster_count BETWEEN 3 AND 8 AND uprn_per_acre <= 0.10 THEN 'C_dispersed_estate'
  ELSE 'X_exclude'
END AS cohort
FROM parcel_features f;

-- 8) Sanity: interior must never exceed total
SELECT COUNT(*) AS bad
FROM   parcel_features
WHERE  uprn_interior_count > uprn_count;
SQL
