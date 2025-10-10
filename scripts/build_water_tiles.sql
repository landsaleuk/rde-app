SET statement_timeout = 0;
SET lock_timeout = '5s';
SET work_mem = '1GB';
SET maintenance_work_mem = '2GB';

DROP MATERIALIZED VIEW IF EXISTS os_water_tiles;

CREATE MATERIALIZED VIEW os_water_tiles AS
WITH grid AS (
  -- 10 km square grid over GB land mask envelope
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
      ST_Buffer(
        ST_Intersection(w.geom, gr.geom), 0.0   -- heal
      ),
      0.25                                       -- de-sliver. Increase to 0.5 if needed.
    ) AS geom
  FROM os_water_simpl w
  JOIN grid gr
    ON w.geom && gr.geom
   AND ST_Intersects(w.geom, gr.geom)
  WHERE NOT ST_IsEmpty(ST_Intersection(w.geom, gr.geom))
),
enumerated AS (
  -- number rows per tile to form chunks
  SELECT
    tile_id,
    geom,
    row_number() OVER (PARTITION BY tile_id ORDER BY ST_Area(geom) DESC NULLS LAST) AS rn
  FROM parts
),
chunked AS (
  -- group into chunks of 500 geoms per tile (tune chunk size if needed)
  SELECT
    tile_id,
    ((rn - 1) / 500) + 1 AS grp,
    geom
  FROM enumerated
),
u1 AS (
  -- first-stage local union per (tile, grp) with healing
  SELECT
    tile_id,
    grp,
    ST_Buffer(
      ST_UnaryUnion(ST_Collect(geom)),
      0.0
    ) AS geom
  FROM chunked
  GROUP BY tile_id, grp
),
u2 AS (
  -- second-stage union to one geometry per tile, with healing
  SELECT
    tile_id,
    ST_Buffer(
      ST_UnaryUnion(ST_Collect(geom)),
      0.0
    ) AS geom
  FROM u1
  GROUP BY tile_id
)
SELECT
  ST_Multi(ST_CollectionExtract(geom, 3))::geometry(MultiPolygon,27700) AS geom
FROM u2
WHERE NOT ST_IsEmpty(geom) AND ST_IsValid(geom);

CREATE INDEX os_water_tiles_gix ON os_water_tiles USING GIST (geom);
ANALYZE os_water_tiles;