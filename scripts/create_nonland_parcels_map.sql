-- A light-weight map layer of all parcels excluded by the non-land gate.
-- Uses nfl_gate (one row per parcel_id) to avoid any duplication.
-- Chooses a single "primary" reason per parcel for easy styling.

DROP MATERIALIZED VIEW IF EXISTS nonland_parcels_map;

WITH reasons_ranked AS (
  SELECT
    parcel_id,
    nonland_reason,
    water_ratio,
    road_ratio,
    rail_ratio,
    land_ratio,
    CASE
      WHEN nonland_reason = 'offshore/land<10%' THEN 1
      WHEN nonland_reason = 'water>=50%'        THEN 2
      WHEN nonland_reason = 'road corridor'     THEN 3
      WHEN nonland_reason = 'rail corridor'     THEN 4
      WHEN nonland_reason = 'long-thin'         THEN 5
      ELSE 99
    END AS pri
  FROM parcel_nonland_flags
),
primary_reason AS (
  -- pick the highest-priority reason per parcel (one row)
  SELECT DISTINCT ON (parcel_id)
         parcel_id, nonland_reason, water_ratio, road_ratio, rail_ratio, land_ratio
  FROM reasons_ranked
  WHERE nonland_reason IS NOT NULL
  ORDER BY parcel_id, pri
)

CREATE MATERIALIZED VIEW nonland_parcels_map AS
SELECT
  p.parcel_id,
  (p.area_sqm/4046.8564224)::numeric(12,1) AS acres,
  pr.nonland_reason,                 -- e.g. 'offshore/land<10%', 'road corridor', ...
  pr.water_ratio, pr.road_ratio, pr.rail_ratio, pr.land_ratio,
  (p.geom_gen)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN nfl_gate ng USING (parcel_id)             -- one row per parcel_id
LEFT JOIN primary_reason pr USING (parcel_id)  -- reason is optional but helpful
WHERE ng.is_nonland;                           -- only the excluded parcels

-- Indexes for fast tiling & filtering
CREATE INDEX nonland_parcels_map_gix        ON nonland_parcels_map USING GIST (geom);
CREATE INDEX nonland_parcels_map_reason_ix  ON nonland_parcels_map (nonland_reason);
ANALYZE nonland_parcels_map;
