SET max_parallel_workers_per_gather = 0;  SET jit = off;

DROP MATERIALIZED VIEW IF EXISTS cohort_parcels_map;

CREATE MATERIALIZED VIEW cohort_parcels_map AS
SELECT
  p.parcel_id,
  c.cohort,
  c.acres,
  c.uprn_count, c.uprn_interior_count, c.uprn_boundary_count,
  c.water_pct, c.land_pct, c.road_ratio, c.rail_ratio,
  c.is_offshore, c.is_road_corridor, c.is_rail_corridor, c.is_roadlike_longthin,
  (p.geom_gen)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN parcel_catalog c USING (parcel_id);

CREATE INDEX cohort_parcels_map_gix       ON cohort_parcels_map USING GIST (geom);
CREATE INDEX cohort_parcels_map_cohort_ix ON cohort_parcels_map (cohort);
ANALYZE cohort_parcels_map;