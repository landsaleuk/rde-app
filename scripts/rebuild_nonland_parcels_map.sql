SET statement_timeout = 0;
SET lock_timeout = '5s';
SET work_mem = '256MB';
SET maintenance_work_mem = '512MB';

-- Pick the flags source that exists (MV or table)
DO $$
BEGIN
  IF to_regclass('public.parcel_nonland_flags') IS NOT NULL THEN
    EXECUTE $$CREATE OR REPLACE VIEW parcel_nonland_flags_v AS
             SELECT parcel_id, is_nonland, nonland_reason
             FROM public.parcel_nonland_flags$$;
  ELSIF to_regclass('public.parcel_nonland_flags_tbl') IS NOT NULL THEN
    EXECUTE $$CREATE OR REPLACE VIEW parcel_nonland_flags_v AS
             SELECT parcel_id, is_nonland, nonland_reason
             FROM public.parcel_nonland_flags_tbl$$;
  ELSE
    RAISE EXCEPTION 'No parcel_nonland_flags* source found';
  END IF;
END$$;

DROP MATERIALIZED VIEW IF EXISTS nonland_parcels_map;

-- Choose a single primary reason per parcel_id and build a light map MV
CREATE MATERIALIZED VIEW nonland_parcels_map AS
WITH ranked AS (
  SELECT
    f.parcel_id,
    CASE
      WHEN f.nonland_reason IN ('offshore/land<5%','offshore/land<10%') THEN 1
      WHEN f.nonland_reason = 'road corridor' THEN 2
      WHEN f.nonland_reason = 'rail corridor' THEN 3
      WHEN f.nonland_reason = 'long-thin'     THEN 4
      ELSE 99
    END AS pri,
    COALESCE(f.nonland_reason,'other') AS reason
  FROM parcel_nonland_flags_v f
  WHERE f.is_nonland
),
primary_reason AS (
  SELECT DISTINCT ON (parcel_id)
         parcel_id, reason
  FROM ranked
  ORDER BY parcel_id, pri
)
SELECT
  p.parcel_id,
  (p.area_sqm/4046.8564224)::numeric(12,1) AS acres,
  pr.reason AS nonland_reason,
  (p.geom_gen)::geometry(MultiPolygon,27700) AS geom
FROM parcel_1acre p
JOIN primary_reason pr USING (parcel_id);

CREATE INDEX nonland_parcels_map_gix ON nonland_parcels_map USING GIST (geom);
CREATE INDEX nonland_parcels_map_reason_ix ON nonland_parcels_map (nonland_reason);
ANALYZE nonland_parcels_map;