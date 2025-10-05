-- =====================================================================
-- Create pg_tileserv-friendly map layers for "removed" parcels
--  - nonland_parcels_map: removed by non-land flags (water/roads/rail/long-thin)
--  - excluded_parcels_map: everything in X_exclude (including non-land & dense)
-- =====================================================================

SET statement_timeout = 0;
SET lock_timeout = '5s';
SET max_parallel_workers_per_gather = 0;  -- keep preview light
SET jit = off;

-- ---------- A) Non-land only -----------------------------------------
DROP MATERIALIZED VIEW IF EXISTS nonland_parcels_map;

CREATE MATERIALIZED VIEW nonland_parcels_map AS
SELECT
  p.parcel_id,
  (p.area_sqm/4046.8564224)::numeric(12,1) AS acres,
  f.water_ratio,
  f.road_ratio,
  f.rail_ratio,
  f.compactness,
  f.nonland_reason,
  (p.geom_gen)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN parcel_nonland_flags f USING (parcel_id)
WHERE f.is_nonland = TRUE;

CREATE INDEX nonland_parcels_map_gix ON nonland_parcels_map USING GIST (geom);
ANALYZE nonland_parcels_map;

-- ---------- B) Everything excluded from cohorts (optional) -----------
DROP MATERIALIZED VIEW IF EXISTS excluded_parcels_map;

CREATE MATERIALIZED VIEW excluded_parcels_map AS
SELECT
  p.parcel_id,
  t.cohort,  -- will be 'X_exclude'
  (p.area_sqm/4046.8564224)::numeric(12,1) AS acres,
  s.uprn_count,
  s.uprn_interior_count,
  s.uprn_per_acre,
  CASE
    WHEN COALESCE(f.is_nonland,FALSE) THEN CONCAT('nonland: ', COALESCE(f.nonland_reason,''))
    ELSE 'cohort_exclude'  -- dense/urban/etc
  END AS exclude_reason,
  (p.geom_gen)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN target_cohorts t USING (parcel_id)
LEFT JOIN parcel_uprn_stats s USING (parcel_id)
LEFT JOIN parcel_nonland_flags f USING (parcel_id)
WHERE t.cohort = 'X_exclude';

CREATE INDEX excluded_parcels_map_gix ON excluded_parcels_map USING GIST (geom);
ANALYZE excluded_parcels_map;

-- quick counts you’ll see in the console
SELECT 'nonland_parcels_map' AS layer, COUNT(*) AS n FROM nonland_parcels_map
UNION ALL
SELECT 'excluded_parcels_map', COUNT(*) FROM excluded_parcels_map;
