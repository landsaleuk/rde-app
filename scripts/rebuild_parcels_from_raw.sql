DROP TABLE IF EXISTS parcels CASCADE;

CREATE TABLE parcels (
  parcel_id BIGSERIAL PRIMARY KEY,
  geom      geometry(MultiPolygon,27700) NOT NULL,
  area_sqm  DOUBLE PRECISION
);

INSERT INTO parcels (geom, area_sqm)
SELECT ST_Multi(ST_CollectionExtract(ST_MakeValid(geom),3)) AS geom,
       ST_Area(ST_Multi(ST_CollectionExtract(ST_MakeValid(geom),3)))   AS area_sqm
FROM raw_inspire
WHERE geom IS NOT NULL;

CREATE INDEX parcels_gix     ON parcels USING GIST (geom);
CREATE INDEX parcels_area_ix ON parcels (area_sqm);
ANALYZE parcels;
