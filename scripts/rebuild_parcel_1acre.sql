DROP MATERIALIZED VIEW IF EXISTS parcel_1acre;

CREATE MATERIALIZED VIEW parcel_1acre AS
SELECT parcel_id,
       geom,
       ST_SimplifyPreserveTopology(geom, 1.0) AS geom_gen,
       area_sqm,
       area_sqm/4046.8564224 AS acres
FROM parcels
WHERE area_sqm >= 4046.8564224;

CREATE INDEX parcel_1acre_geom_gix    ON parcel_1acre USING GIST (geom);
CREATE INDEX parcel_1acre_geomgen_gix ON parcel_1acre USING GIST (geom_gen);
ANALYZE parcel_1acre;
