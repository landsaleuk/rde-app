-- 1) Base: UPRNs covered by each parcel
DROP TABLE IF EXISTS parcel_uprn_base;
CREATE TABLE parcel_uprn_base AS
SELECT p.parcel_id, p.geom AS pgeom, p.area_sqm, u.uprn, u.geom AS ugeom
FROM parcel_1acre p
JOIN os_open_uprn u
  ON ST_Covers(p.geom, u.geom);
CREATE INDEX pub_parcel_ix ON parcel_uprn_base (parcel_id);
CREATE INDEX pub_uprn_ix   ON parcel_uprn_base (uprn);
CREATE INDEX pub_gix       ON parcel_uprn_base USING GIST (pgeom);
CREATE INDEX pub_ugix      ON parcel_uprn_base USING GIST (ugeom);
ANALYZE parcel_uprn_base;

-- 2) Stats with 5 m interior shrink
DROP TABLE IF EXISTS parcel_uprn_stats;
CREATE TABLE parcel_uprn_stats AS
WITH shrink AS (
  SELECT parcel_id, ST_Buffer(pgeom, -5) AS i_geom, area_sqm
  FROM (SELECT DISTINCT parcel_id, pgeom, area_sqm FROM parcel_uprn_base) d
  UNION
  SELECT parcel_id, ST_Buffer(geom, -5) AS i_geom, area_sqm
  FROM parcel_1acre
  WHERE parcel_id NOT IN (SELECT parcel_id FROM parcel_uprn_base)
)
SELECT
  s.parcel_id,
  (s.area_sqm/4046.8564224)::numeric(12,2) AS acres,
  COALESCE(COUNT(b.uprn),0)::int AS uprn_count,
  COALESCE(COUNT(b.uprn) FILTER (
           WHERE NOT ST_IsEmpty(s.i_geom) AND ST_Covers(s.i_geom, b.ugeom)
         ),0)::int AS uprn_interior_count,
  (COALESCE(COUNT(b.uprn),0)/NULLIF(s.area_sqm/4046.8564224,0))::numeric(10,4) AS uprn_per_acre
FROM shrink s
LEFT JOIN parcel_uprn_base b ON b.parcel_id = s.parcel_id
GROUP BY s.parcel_id, s.i_geom, s.area_sqm;
CREATE INDEX parcel_uprn_stats_ix ON parcel_uprn_stats (parcel_id);
ANALYZE parcel_uprn_stats;

-- 3) Cluster UPRNs (homestead vs hamlet)
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

-- 4) Features + cohorts
DROP MATERIALIZED VIEW IF EXISTS parcel_features;
CREATE MATERIALIZED VIEW parcel_features AS
SELECT s.parcel_id, s.acres, s.uprn_count, s.uprn_interior_count,
       COALESCE(c.uprn_cluster_count,0) AS uprn_cluster_count,
       s.uprn_per_acre
FROM parcel_uprn_stats s
LEFT JOIN parcel_uprn_clusters c USING (parcel_id);
CREATE INDEX parcel_features_ix ON parcel_features (parcel_id);

DROP MATERIALIZED VIEW IF EXISTS target_cohorts;
CREATE MATERIALIZED VIEW target_cohorts AS
SELECT f.*,
CASE
  WHEN uprn_interior_count = 0 THEN 'A_bare_land'
  WHEN uprn_cluster_count <= 2 AND uprn_per_acre <= 0.05 THEN 'B_single_holding'
  WHEN uprn_cluster_count BETWEEN 3 AND 8 AND uprn_per_acre <= 0.10 THEN 'C_dispersed_estate'
  ELSE 'X_exclude'
END AS cohort
FROM parcel_features f;

-- 5) Sanity: interior must never exceed total
SELECT COUNT(*) AS bad
FROM parcel_features
WHERE uprn_interior_count > uprn_count;
