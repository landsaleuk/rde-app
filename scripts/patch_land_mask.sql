DROP MATERIALIZED VIEW IF EXISTS gb_land_mask;

-- Close tile seams: buffer OUT (eps), union, buffer IN (eps) back to coast.
-- If you still see seam artifacts later, raise eps to 12.0.
CREATE MATERIALIZED VIEW gb_land_mask AS
SELECT
  ST_CollectionExtract(
    ST_Buffer(
      ST_UnaryUnion( ST_Collect(geom) ),  -- aggregate all os_land rows first
      8.0
    ),
    -8.0
  )::geometry(MultiPolygon,27700) AS geom
FROM os_land;

CREATE INDEX gb_land_mask_gix ON gb_land_mask USING GIST (geom);
ANALYZE gb_land_mask;

-- Sanity checks (1 row expected)
-- SELECT COUNT(*) FROM gb_land_mask;
-- SELECT ST_NumGeometries(geom) FROM gb_land_mask;  -- can be many parts, but 1 row
