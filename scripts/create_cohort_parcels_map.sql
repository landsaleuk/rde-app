-- Optional: reduce shared memory pressure if the db container has small /dev/shm
SET max_parallel_workers_per_gather = 0;
SET jit = off;

DROP MATERIALIZED VIEW IF EXISTS cohort_parcels_map;

CREATE MATERIALIZED VIEW cohort_parcels_map AS
SELECT
  p.parcel_id,
  t.cohort,                                         -- A_bare_land / B_single_holding / C_dispersed_estate
  (p.area_sqm/4046.8564224)::numeric(12,1) AS acres,
  s.uprn_count,
  s.uprn_interior_count,
  s.uprn_per_acre,
  (p.geom_gen)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN target_cohorts t USING (parcel_id)
LEFT JOIN parcel_uprn_stats s USING (parcel_id)
WHERE t.cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate');

CREATE INDEX IF NOT EXISTS cohort_parcels_map_gix ON cohort_parcels_map USING GIST (geom);
CREATE INDEX IF NOT EXISTS cohort_parcels_map_cohort_ix ON cohort_parcels_map (cohort);
ANALYZE cohort_parcels_map;