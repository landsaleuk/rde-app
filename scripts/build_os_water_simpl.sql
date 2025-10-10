SET statement_timeout = 0;
SET lock_timeout = '5s';

DROP TABLE IF EXISTS os_water_simpl;

CREATE UNLOGGED TABLE os_water_simpl AS
SELECT
  -- simplify → heal → keep polygons only
  ST_CollectionExtract(
    ST_Buffer(
      ST_SimplifyPreserveTopology(ST_MakeValid(geom), 20),  -- tolerance (m). Use 10.0 if you still see issues.
      0.0
    ),
    3
  )::geometry(MultiPolygon,27700) AS geom
FROM os_water
WHERE NOT ST_IsEmpty(geom);

CREATE INDEX os_water_simpl_gix ON os_water_simpl USING GIST (geom);
ANALYZE os_water_simpl;