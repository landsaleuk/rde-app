SET statement_timeout = 0;
SET lock_timeout = '5s';
SET work_mem = '512MB';
SET maintenance_work_mem = '512MB';

-- Optional: keep a single-row mask (handy for maps/QA), but we won't use it for intersections
DROP MATERIALIZED VIEW IF EXISTS gb_land_mask;
CREATE MATERIALIZED VIEW gb_land_mask AS
WITH raw AS (
  SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
  FROM os_land
),
closed AS (
  -- Close tile seams; bump 15.0 to 20.0 if you still see grid artefacts
  SELECT ST_Buffer(ST_Buffer(geom, 15.0), -15.0) AS geom
  FROM raw
)
SELECT ST_Multi(ST_CollectionExtract(geom,3))::geometry(MultiPolygon,27700) AS geom
FROM closed;
CREATE INDEX gb_land_mask_gix ON gb_land_mask USING GIST (geom);
ANALYZE gb_land_mask;

-- The **workhorse**: subdivide into many small polygons so the join can use the index
DROP MATERIALIZED VIEW IF EXISTS gb_land_mask_tiles;
CREATE MATERIALIZED VIEW gb_land_mask_tiles AS
WITH closed AS (
  SELECT (SELECT geom FROM gb_land_mask) AS geom
)
SELECT
  ST_Multi(ST_CollectionExtract( ST_MakeValid( g ), 3))::geometry(MultiPolygon,27700) AS geom
FROM closed,
     LATERAL ST_Subdivide(closed.geom, 512) AS g;  -- 512 vertices/tile is a good default

CREATE INDEX gb_land_mask_tiles_gix ON gb_land_mask_tiles USING GIST (geom);
ANALYZE gb_land_mask_tiles;

-- Sanity
-- SELECT COUNT(*) rows FROM gb_land_mask;         -- expect 1
-- SELECT COUNT(*) tiles FROM gb_land_mask_tiles;  -- expect many (tens of thousands is normal)