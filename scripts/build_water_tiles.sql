SET statement_timeout = 0;
SET lock_timeout = '5s';
SET work_mem = '512MB';
SET maintenance_work_mem = '512MB';

DROP MATERIALIZED VIEW IF EXISTS os_water_tiles;

-- Dissolve -> make valid -> subdivide into many small tiles
CREATE MATERIALIZED VIEW os_water_tiles AS
WITH dissolved AS (
  SELECT ST_MakeValid(ST_UnaryUnion(ST_Collect(geom))) AS geom
  FROM os_water
)
SELECT
  ST_Multi(ST_CollectionExtract(g,3))::geometry(MultiPolygon,27700) AS geom
FROM dissolved,
     LATERAL ST_Subdivide(dissolved.geom, 512) AS g;

CREATE INDEX os_water_tiles_gix ON os_water_tiles USING GIST (geom);
ANALYZE os_water_tiles;

-- Sanity
-- SELECT COUNT(*) tiles FROM os_water_tiles;