-- Export A/B/C parcels with counts + UPRN ID lists to a CSV
-- Writes to /tmp/targets_abc.csv inside the db container.

\set ON_ERROR_STOP on

COPY (
WITH tc AS (
  SELECT parcel_id, cohort
  FROM target_cohorts
  WHERE cohort IN ('A_bare_land','B_single_holding','C_dispersed_estate')
),

-- Limit UPRN work to A/B/C parcels only
base AS (
  SELECT b.parcel_id, b.uprn, b.ugeom
  FROM parcel_uprn_base b
  JOIN tc USING (parcel_id)
),

-- Interior polygons for those parcels
inter AS (
  SELECT parcel_id, i_geom
  FROM parcel_interior
  WHERE parcel_id IN (SELECT parcel_id FROM tc)
),

-- Aggregate UPRN ID lists (as TEXT arrays so CSV is clean)
ag AS (
  SELECT
    base.parcel_id,
    array_agg(base.uprn::text ORDER BY base.uprn) AS uprns_all,
    array_agg(base.uprn::text ORDER BY base.uprn)
      FILTER (WHERE inter.i_geom IS NOT NULL
              AND base.ugeom && inter.i_geom
              AND ST_Covers(inter.i_geom, base.ugeom))          AS uprns_interior,
    array_agg(base.uprn::text ORDER BY base.uprn)
      FILTER (WHERE NOT (
                inter.i_geom IS NOT NULL
                AND base.ugeom && inter.i_geom
                AND ST_Covers(inter.i_geom, base.ugeom)
              ))                                                AS uprns_boundary
  FROM base
  LEFT JOIN inter ON inter.parcel_id = base.parcel_id
  GROUP BY base.parcel_id
)

SELECT
  tc.parcel_id,
  tc.cohort,
  ROUND(s.acres::numeric, 1)                    AS acres,
  s.uprn_count,
  s.uprn_interior_count,
  COALESCE(c.uprn_cluster_count, 0)             AS uprn_cluster_count,
  s.uprn_per_acre,
  -- centroid in WGS84 for mapping/CRM
  ST_X(ST_Transform(ST_PointOnSurface(p.geom), 4326)) AS lon,
  ST_Y(ST_Transform(ST_PointOnSurface(p.geom), 4326)) AS lat,
  COALESCE(ag.uprns_all,      ARRAY[]::text[])  AS uprns_all,
  COALESCE(ag.uprns_interior, ARRAY[]::text[])  AS uprns_interior,
  COALESCE(ag.uprns_boundary, ARRAY[]::text[])  AS uprns_boundary
FROM tc
JOIN parcel_1acre           p  USING (parcel_id)
LEFT JOIN parcel_uprn_stats s  USING (parcel_id)
LEFT JOIN parcel_uprn_clusters c USING (parcel_id)
LEFT JOIN ag                   USING (parcel_id)
ORDER BY tc.cohort, acres DESC
) TO '/tmp/targets_abc.csv' WITH (FORMAT CSV, HEADER, ENCODING 'UTF8');
