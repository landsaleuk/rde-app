SET statement_timeout = 0;
SET lock_timeout = '5s';
SET work_mem = '1GB';
SET maintenance_work_mem = '2GB';

-- 1) Simplify + validate into an UNLOGGED staging table (fast writes)
DROP TABLE IF EXISTS os_water_simpl;
CREATE UNLOGGED TABLE os_water_simpl AS
SELECT ST_SimplifyPreserveTopology(ST_MakeValid(geom), 5.0) AS geom   -- tolerance in metres (EPSG:27700)
FROM os_water;
CREATE INDEX os_water_simpl_gix ON os_water_simpl USING GIST (geom);
ANALYZE os_water_simpl;

-- 2) Dissolve the simplified geometries, then subdivide into tiles
DROP MATERIALIZED VIEW IF EXISTS os_water_tiles;
CREATE MATERIALIZED VIEW os_water_tiles AS
WITH dissolved AS (
  SELECT ST_MakeValid(ST_UnaryUnion(ST_Collect(geom))) AS geom
  FROM os_water_simpl
)
SELECT
  ST_Multi(ST_CollectionExtract(g,3))::geometry(MultiPolygon,27700) AS geom
FROM dissolved,
     LATERAL ST_Subdivide(dissolved.geom, 512) AS g;

CREATE INDEX os_water_tiles_gix ON os_water_tiles USING GIST (geom);
ANALYZE os_water_tiles;