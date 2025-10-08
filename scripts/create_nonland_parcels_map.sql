-- Show all parcels excluded by non-land rules (water/foreshore, road/rail corridors, long-thin)
-- Uses nfl_gate (deduped) + parcel_1acre (geometry) + a primary reason from parcel_nonland_flags.
-- We pick one reason per parcel by priority so you can color/style in the UI.

DROP MATERIALIZED VIEW IF EXISTS nonland_parcels_map;

WITH reasons_ranked AS (
  SELECT
    parcel_id,
    COALESCE(nonland_reason,'other') AS nonland_reason,
    water_ratio, road_ratio, rail_ratio, land_ratio,
    CASE
      WHEN nonland_reason = 'offshore/land<10%' THEN 1
      WHEN nonland_reason = 'water>=50%'        THEN 2
      WHEN nonland_reason = 'road corridor'     THEN 3
      WHEN nonland_reason = 'rail corridor'     THEN 4
      WHEN nonland_reason = 'long-thin'         THEN 5
      ELSE 99
    END AS pri
  FROM parcel_nonland_flags
  WHERE is_nonland
),
primary_reason AS (
  -- one row per parcel_id, choosing the highest-priority reason
  SELECT DISTINCT ON (parcel_id)
         parcel_id, nonland_reason, water_ratio, road_ratio, rail_ratio, land_ratio
  FROM reasons_ranked
  ORDER BY parcel_id, pri
)
CREATE MATERIALIZED VIEW nonland_parcels_map AS
SELECT
  p.parcel_id,
  (p.area_sqm/4046.8564224)::numeric(12,1) AS acres,
  pr.nonland_reason,
  pr.water_ratio, pr.road_ratio, pr.rail_ratio, pr.land_ratio,
  -- prefer simplified geometry if present; fall back to full geom
  COALESCE(p.geom_gen, p.geom)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN nfl_gate ng USING (parcel_id)             -- one row per parcel_id, deduped
LEFT JOIN primary_reason pr USING (parcel_id)
WHERE ng.is_nonland;

CREATE INDEX nonland_parcels_map_gix        ON nonland_parcels_map USING GIST (geom);
CREATE INDEX nonland_parcels_map_reason_ix  ON nonland_parcels_map (nonland_reason);
ANALYZE nonland_parcels_map;
