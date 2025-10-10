-- 05b_build_parcel_metrics_chunk.sql
-- Run with: psql ... -v min_id=1 -v max_id=100000 -f 05b_build_parcel_metrics_chunk.sql
SET statement_timeout = 0;
SET lock_timeout = '5s';
SET work_mem = '256MB';
SET maintenance_work_mem = '512MB';

WITH base AS (
  SELECT
    p.parcel_id,
    (p.geom_gen)::geometry(MultiPolygon,27700) AS geom,     -- simplified parcel
    ST_Area(p.geom_gen)::double precision       AS a_parcel,
    (p.area_sqm/4046.8564224)::numeric(12,2)    AS acres
  FROM parcel_1acre p
  WHERE p.parcel_id BETWEEN :min_id AND :max_id
),
land AS (
  -- land area via land-mask tiles (fast & robust)
  SELECT b.parcel_id,
         COALESCE(SUM(ST_Area(ST_Intersection(b.geom, t.geom))),0)::double precision AS a_land
  FROM base b
  JOIN gb_land_mask_tiles t
    ON b.geom && t.geom AND ST_Intersects(b.geom, t.geom)
  GROUP BY b.parcel_id
),
-- Local, per-parcel water union (no global dissolve)
water_pieces AS (
  SELECT
    b.parcel_id,
    -- clip → heal → snap to grid to avoid slivers/ring oddities
    ST_SnapToGrid(
      ST_Buffer(ST_Intersection(b.geom, w.geom), 0.0),
      0.5
    ) AS geom
  FROM base b
  JOIN os_water_simpl w
    ON b.geom && w.geom AND ST_Intersects(b.geom, w.geom)
  WHERE NOT ST_IsEmpty(ST_Intersection(b.geom, w.geom))
),
water_union AS (
  SELECT
    parcel_id,
    ST_Buffer( ST_UnaryUnion(ST_Collect(geom)), 0.0 ) AS geom
  FROM water_pieces
  GROUP BY parcel_id
),
r AS (
  SELECT b.parcel_id,
         COALESCE(SUM(ST_Area(ST_Intersection(b.geom, rb.geom))),0)::double precision AS a_road
  FROM base b
  LEFT JOIN roads_buf rb
    ON b.geom && rb.geom AND ST_Intersects(b.geom, rb.geom)
  GROUP BY b.parcel_id
),
rl AS (
  SELECT b.parcel_id,
         COALESCE(SUM(ST_Area(ST_Intersection(b.geom, rlb.geom))),0)::double precision AS a_rail
  FROM base b
  LEFT JOIN rail_buf rlb
    ON b.geom && rlb.geom AND ST_Intersects(b.geom, rlb.geom)
  GROUP BY b.parcel_id
),
centroid AS (
  -- Offshore if centroid not covered by ANY land tile
  SELECT b.parcel_id,
         CASE
           WHEN EXISTS (
             SELECT 1 FROM gb_land_mask_tiles t
             WHERE t.geom && ST_Expand(ST_PointOnSurface(b.geom), 1)
               AND ST_Covers(t.geom, ST_PointOnSurface(b.geom))
           ) THEN FALSE ELSE TRUE
         END AS is_offshore
  FROM base b
)
INSERT INTO parcel_metrics_tbl AS t (
  parcel_id, acres, land_ratio, water_ratio, road_ratio, rail_ratio,
  water_pct, land_pct, compactness,
  is_offshore, is_road_corridor, is_rail_corridor, is_roadlike_longthin
)
SELECT
  b.parcel_id,
  b.acres,
  (land.a_land / NULLIF(b.a_parcel,0))::numeric(6,4)                            AS land_ratio,
  (COALESCE(ST_Area(wu.geom),0) / NULLIF(b.a_parcel,0))::numeric(6,4)           AS water_ratio,
  (r.a_road  / NULLIF(b.a_parcel,0))::numeric(6,4)                              AS road_ratio,
  (rl.a_rail / NULLIF(b.a_parcel,0))::numeric(6,4)                              AS rail_ratio,
  ROUND(100 * (COALESCE(ST_Area(wu.geom),0) / NULLIF(b.a_parcel,0))::numeric,1) AS water_pct,
  ROUND(100 * (land.a_land / NULLIF(b.a_parcel,0))::numeric,1)                  AS land_pct,
  (4*PI()*b.a_parcel / NULLIF( ST_Perimeter(b.geom)::double precision
                               * ST_Perimeter(b.geom)::double precision, 0))::numeric(6,4) AS compactness,
  c.is_offshore                                                                  AS is_offshore,
  ((r.a_road  / NULLIF(b.a_parcel,0)) >= 0.65 AND b.acres <= 40)                 AS is_road_corridor,
  ((rl.a_rail / NULLIF(b.a_parcel,0)) >= 0.50 AND b.acres <= 60)                 AS is_rail_corridor,
  (b.acres <= 30 AND
   (4*PI()*b.a_parcel / NULLIF( ST_Perimeter(b.geom)::double precision
                               * ST_Perimeter(b.geom)::double precision, 0)) < 0.030)       AS is_roadlike_longthin
FROM base b
LEFT JOIN land        ON land.parcel_id = b.parcel_id
LEFT JOIN water_union wu ON wu.parcel_id = b.parcel_id
LEFT JOIN r           ON r.parcel_id    = b.parcel_id
LEFT JOIN rl          ON rl.parcel_id   = b.parcel_id
LEFT JOIN centroid c  ON c.parcel_id    = b.parcel_id
ON CONFLICT (parcel_id) DO UPDATE
SET acres               = EXCLUDED.acres,
    land_ratio          = EXCLUDED.land_ratio,
    water_ratio         = EXCLUDED.water_ratio,
    road_ratio          = EXCLUDED.road_ratio,
    rail_ratio          = EXCLUDED.rail_ratio,
    water_pct           = EXCLUDED.water_pct,
    land_pct            = EXCLUDED.land_pct,
    compactness         = EXCLUDED.compactness,
    is_offshore         = EXCLUDED.is_offshore,
    is_road_corridor    = EXCLUDED.is_road_corridor,
    is_rail_corridor    = EXCLUDED.is_rail_corridor,
    is_roadlike_longthin= EXCLUDED.is_roadlike_longthin;
