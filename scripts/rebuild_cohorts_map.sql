DROP MATERIALIZED VIEW IF EXISTS cohort_parcels_map;
DROP MATERIALIZED VIEW IF EXISTS target_cohorts;

-- Cohorts (gated by nfl_gate)
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
JOIN nfl_gate ng USING (parcel_id);     -- << one row per parcel id, no ambiguity

CREATE INDEX target_cohorts_cohort_ix ON target_cohorts (cohort);
CREATE INDEX target_cohorts_pid_ix    ON target_cohorts (parcel_id);
ANALYZE target_cohorts;

-- Map (hard‑gated by nfl_gate)
CREATE MATERIALIZED VIEW cohort_parcels_map AS
SELECT
  p.parcel_id,
  t.cohort,
  (p.area_sqm/4046.8564224)::numeric(12,1) AS acres,
  s.uprn_count, s.uprn_interior_count, s.uprn_per_acre,
  (p.geom_gen)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN target_cohorts t USING (parcel_id)
JOIN nfl_gate ng USING (parcel_id)         -- << hard gate via aggregated flag
LEFT JOIN parcel_uprn_stats s USING (parcel_id)
WHERE t.cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate')
  AND NOT ng.is_nonland;

CREATE INDEX cohort_parcels_map_gix       ON cohort_parcels_map USING GIST (geom);
CREATE INDEX cohort_parcels_map_cohort_ix ON cohort_parcels_map (cohort);
ANALYZE cohort_parcels_map;
