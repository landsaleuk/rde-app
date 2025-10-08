-- scripts/patch_land_mask.sql
-- Build a single-row GB land mask and close tile seams
SET statement_timeout = 0;
SET lock_timeout = '5s';
SET work_mem = '512MB';
SET maintenance_work_mem = '512MB';

DROP MATERIALIZED VIEW IF EXISTS gb_land_mask;

CREATE MATERIALIZED VIEW gb_land_mask AS
WITH raw AS (
  -- aggregate all os_land rows first (one big geometry)
  SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
  FROM os_land
),
closed AS (
  -- "close" the tile seams: buffer OUT then IN (tune eps if needed)
  SELECT ST_Buffer(ST_Buffer(geom, 12.0), -12.0) AS geom
  FROM raw
)
SELECT
  ST_Multi(ST_CollectionExtract(geom, 3))::geometry(MultiPolygon,27700) AS geom
FROM closed;

CREATE INDEX gb_land_mask_gix ON gb_land_mask USING GIST (geom);
ANALYZE gb_land_mask;
