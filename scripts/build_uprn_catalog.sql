SET statement_timeout = 0;
SET lock_timeout = '5s';
SET work_mem = '256MB';
SET maintenance_work_mem = '512MB';

-- Safety: helpful indexes if missing
CREATE INDEX IF NOT EXISTS parcel_uprn_base_uprn_ix ON parcel_uprn_base (uprn);
CREATE INDEX IF NOT EXISTS parcel_uprn_base_pid_ix  ON parcel_uprn_base (parcel_id);
DO $$
BEGIN
  IF to_regclass('public.parcel_interior') IS NOT NULL THEN
    IF NOT EXISTS (
      SELECT 1 FROM pg_indexes
      WHERE schemaname='public' AND tablename='parcel_interior' AND indexname='parcel_interior_gix'
    ) THEN
      EXECUTE 'CREATE INDEX parcel_interior_gix ON parcel_interior USING GIST (i_geom)';
    END IF;
  END IF;
END
$$ LANGUAGE plpgsql;

DROP MATERIALIZED VIEW IF EXISTS uprn_catalog;

-- Choose a primary parcel for each UPRN: prefer interior hit; else any containing parcel
CREATE MATERIALIZED VIEW uprn_catalog AS
WITH all_hits AS (
  SELECT b.uprn, b.parcel_id, b.ugeom
  FROM parcel_uprn_base b
),
agg_parcels AS (
  SELECT uprn, array_agg(DISTINCT parcel_id ORDER BY parcel_id) AS parcel_ids
  FROM all_hits
  GROUP BY uprn
),
primary_pick AS (
  -- If parcel_interior exists, use it to prioritize interior hits; else fallback to minimal parcel_id
  SELECT DISTINCT ON (h.uprn)
         h.uprn,
         h.parcel_id,
         CASE
           WHEN to_regclass('public.parcel_interior') IS NOT NULL THEN
             EXISTS (
               SELECT 1
               FROM parcel_interior i
               WHERE i.parcel_id = h.parcel_id
                 AND ST_Covers(i.i_geom, h.ugeom)
             )
           ELSE FALSE
         END AS is_interior
  FROM all_hits h
  LEFT JOIN parcel_interior i ON i.parcel_id = h.parcel_id
  ORDER BY h.uprn,
           -- interior first if possible
           (CASE WHEN i.i_geom IS NOT NULL AND ST_Covers(i.i_geom, h.ugeom) THEN 1 ELSE 0 END) DESC,
           h.parcel_id
),
joined AS (
  SELECT
    p.uprn,
    p.parcel_id,
    p.is_interior,
    c.cohort,
    c.acres,
    c.uprn_interior_count,                -- UPRNs (interior) in that parcel
    m.water_pct, m.land_pct,
    m.is_road_corridor, m.is_rail_corridor, m.is_offshore
  FROM primary_pick p
  JOIN parcel_catalog  c ON c.parcel_id = p.parcel_id
  LEFT JOIN parcel_metrics m ON m.parcel_id = p.parcel_id
)
SELECT
  j.uprn,
  j.parcel_id,                -- primary parcel
  a.parcel_ids,               -- all parcels containing this UPRN
  j.cohort,
  j.acres,
  j.uprn_interior_count,      -- interior UPRNs in the parcel
  j.is_interior,              -- whether THIS UPRN is interior in the primary parcel
  j.water_pct, j.land_pct,
  j.is_road_corridor, j.is_rail_corridor, j.is_offshore
FROM joined j
JOIN agg_parcels a USING (uprn);

CREATE UNIQUE INDEX uprn_catalog_pk ON uprn_catalog (uprn);
CREATE INDEX uprn_catalog_pid_ix    ON uprn_catalog (parcel_id);
CREATE INDEX uprn_catalog_cohort_ix ON uprn_catalog (cohort);
ANALYZE uprn_catalog;
