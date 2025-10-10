SET statement_timeout = 0;
SET lock_timeout = '5s';

DROP MATERIALIZED VIEW IF EXISTS parcel_catalog;

CREATE MATERIALIZED VIEW parcel_catalog AS
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
SELECT
  f.parcel_id,
  -- Cohort (no gating)
  CASE
    WHEN f.uprn_interior_count = 0 THEN 'A_bare_land'
    WHEN f.acres BETWEEN 1 AND 5
         AND f.uprn_interior_count BETWEEN 1 AND 3
         AND f.uprn_cluster_count <= 3
         AND f.interior_share >= 0.60
      THEN 'B_single_holding'
    WHEN f.acres > 5 AND f.acres <= 25
         AND f.uprn_interior_count <= 2
         AND f.uprn_cluster_count <= 2
         AND f.interior_share >= 0.60
      THEN 'B_single_holding'
    WHEN f.acres > 25 AND f.uprn_cluster_count <= 2 AND f.uprn_per_acre <= 0.05
      THEN 'B_single_holding'
    WHEN f.uprn_cluster_count BETWEEN 3 AND 8 AND f.uprn_per_acre <= 0.10
      THEN 'C_dispersed_estate'
    ELSE 'X_exclude'
  END AS cohort,

  -- UPRN stats (as before)
  f.acres,
  f.uprn_count,
  f.uprn_interior_count,
  (f.uprn_count - f.uprn_interior_count) AS uprn_boundary_count,

  -- NEW: metrics
  m.water_pct, m.land_pct,
  m.water_ratio, m.road_ratio, m.rail_ratio, m.compactness,
  m.is_offshore, m.is_road_corridor, m.is_rail_corridor, m.is_roadlike_longthin

FROM f
LEFT JOIN parcel_metrics m USING (parcel_id);

CREATE INDEX parcel_catalog_pid_ix    ON parcel_catalog (parcel_id);
CREATE INDEX parcel_catalog_cohort_ix ON parcel_catalog (cohort);
ANALYZE parcel_catalog;