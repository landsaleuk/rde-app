-- Rebuild ONLY the cohorts MV with improved rules
--  - Adds a 'smallholding' clause so 1–5 acre parcels with 1–3 interior UPRNs are included
--  - Keeps the original sparse-density B rule for larger holdings
--  - Keeps C (dispersed estates) and A (bare land) as before

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
    -- A) Bare land / woodland (no interior UPRNs)
    WHEN uprn_interior_count = 0
      THEN 'A_bare_land'

    -- B) Smallholding (NEW): 1–5 acres, 1–3 interior UPRNs, <=3 clusters, most points truly interior
    WHEN acres BETWEEN 1 AND 5
         AND uprn_interior_count BETWEEN 1 AND 3
         AND uprn_cluster_count <= 3
         AND interior_share >= 0.60
      THEN 'B_single_holding'

    -- B) Sparse (existing): larger holdings with very low UPRN density
    WHEN uprn_cluster_count <= 2
         AND uprn_per_acre <= 0.05        -- ≈ 1 per 20 acres
      THEN 'B_single_holding'

    -- C) Dispersed estate (existing): several clusters but still rural density
    WHEN uprn_cluster_count BETWEEN 3 AND 8
         AND uprn_per_acre <= 0.10        -- ≤ 1 per 10 acres
      THEN 'C_dispersed_estate'

    -- X) Everything else (parks, hamlets, campuses, urban fringe, dense)
    ELSE 'X_exclude'
  END AS cohort
FROM f;

-- Helpful indexes
CREATE INDEX target_cohorts_cohort_ix ON target_cohorts (cohort);
CREATE INDEX target_cohorts_pid_ix    ON target_cohorts (parcel_id);
-- Optional: speed range queries by size
-- CREATE INDEX target_cohorts_acres_ix ON target_cohorts (acres);

ANALYZE target_cohorts;