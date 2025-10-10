SET statement_timeout = 0;
SET lock_timeout = '5s';
SET work_mem = '1GB';
SET maintenance_work_mem = '2GB';

DROP MATERIALIZED VIEW IF EXISTS os_water_tiles;

CREATE MATERIALIZED VIEW os_water_tiles AS
WITH grid AS (
  -- 10 km square grid over the GB land-mask envelope
  SELECT row_number() OVER ()::int AS tile_id, g.geom
  FROM ST_SquareGrid(
         10000,  -- metres
         (SELECT ST_Envelope(geom) FROM gb_land_mask)
       ) AS g(geom)
),
parts AS (
  -- Clip simplified water to each grid cell; clean to avoid ring/hole oddities & slivers
  SELECT
    gr.tile_id,
    ST_SnapToGrid(
      ST_Buffer(ST_Intersection(w.geom, gr.geom), 0.0),
      0.1
    ) AS geom
  FROM os_water_simpl w
  JOIN grid gr
    ON w.geom && gr.geom
   AND ST_Intersects(w.geom, gr.geom)
),
diss AS (
  -- Local dissolve per cell (robust & fast)
  SELECT
    tile_id,
    ST_MakeValid(ST_UnaryUnion(ST_Collect(geom))) AS geom
  FROM parts
  GROUP BY tile_id
)
SELECT
  ST_Multi(ST_CollectionExtract(geom,3))::geometry(MultiPolygon,27700) AS geom
FROM diss
WHERE NOT ST_IsEmpty(geom);

CREATE INDEX os_water_tiles_gix ON os_water_tiles USING GIST (geom);
ANALYZE os_water_tiles;