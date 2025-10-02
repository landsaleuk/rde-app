-- =====================================================================
-- Rebuild cohorts + the dependent map with improved B rules:
--  * B1 1–5 acres: 1–3 interior UPRNs, <=3 clusters (smallholdings)
--  * B2 6–25 acres: <=2 interior UPRNs, <=2 clusters (mid-size homestead)
--  * B3 >25 acres: very sparse density as before (<=1 per 20 acres)
-- =====================================================================

SET statement_timeout = 0;
SET lock_timeout = '5s';

-- Drop dependent MV first, otherwise target_cohorts cannot be dropped
DROP MATERIALIZED VIEW IF EXISTS cohort_parcels_map;

-- ----- Rebuild cohorts ------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS target_cohorts;

CREATE MATERIALIZED VIEW target_cohorts AS
WITH f AS (
  SELECT
    parcel_id,
    acres,
    uprn_count,
    uprn_interior_count,
    uprn_per_acre,
    COALESCE(uprn_cluster_count, 0) AS uprn_cluster_count,
    CASE WHEN uprn_count > 0
         THEN uprn_interior_count::float / uprn_count
         ELSE 0 END AS interior_share
  FROM parcel_features
)
SELECT
  f.*,
  CASE
    -- A) Bare land / woodland
    WHEN uprn_interior_count = 0
      THEN 'A_bare_land'

    -- B1) Smallholding: 1–5 acres, 1–3 interior UPRNs, <=3 clusters, mostly interior
    WHEN acres BETWEEN 1 AND 5
         AND uprn_interior_count BETWEEN 1 AND 3
         AND uprn_cluster_count <= 3
         AND interior_share >= 0.60
      THEN 'B_single_holding'

    -- B2) Mid-size homestead: 6–25 acres, very few interior UPRNs (<=2), low clustering
    WHEN acres > 5 AND acres <= 25
         AND uprn_interior_count <= 2
         AND uprn_cluster_count <= 2
         AND interior_share >= 0.60
      THEN 'B_single_holding'

    -- B3) Large sparse: >25 acres, very low UPRN density and low clustering
    WHEN acres > 25
         AND uprn_cluster_count <= 2
         AND uprn_per_acre <= 0.05      -- ≈ 1 per 20 acres
      THEN 'B_single_holding'

    -- C) Dispersed estates (several clusters but still rural density)
    WHEN uprn_cluster_count BETWEEN 3 AND 8
         AND uprn_per_acre <= 0.10      -- ≤ 1 per 10 acres
      THEN 'C_dispersed_estate'

    -- X) Everything else (parks, hamlets, campuses, urban fringe, dense)
    ELSE 'X_exclude'
  END AS cohort
FROM f;

CREATE INDEX target_cohorts_cohort_ix ON target_cohorts (cohort);
CREATE INDEX target_cohorts_pid_ix    ON target_cohorts (parcel_id);
ANALYZE target_cohorts;

-- ----- Rebuild the map MV that depends on cohorts --------------------
SET max_parallel_workers_per_gather = 0;
SET jit = off;

CREATE MATERIALIZED VIEW cohort_parcels_map AS
SELECT
  p.parcel_id,
  t.cohort,
  (p.area_sqm/4046.8564224)::numeric(12,1) AS acres,
  s.uprn_count,
  s.uprn_interior_count,
  s.uprn_per_acre,
  (p.geom_gen)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN target_cohorts t USING (parcel_id)
LEFT JOIN parcel_uprn_stats s USING (parcel_id)
WHERE t.cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate');

CREATE INDEX IF NOT EXISTS cohort_parcels_map_gix       ON cohort_parcels_map USING GIST (geom);
CREATE INDEX IF NOT EXISTS cohort_parcels_map_cohort_ix ON cohort_parcels_map (cohort);
ANALYZE cohort_parcels_map;

-- Sanity: interior must never exceed total (0 rows expected)
SELECT COUNT(*) AS bad
FROM target_cohorts
WHERE uprn_interior_count > uprn_count;
