SET statement_timeout = 0;
SET lock_timeout = '5s';

DROP TABLE IF EXISTS os_water_simpl;

CREATE UNLOGGED TABLE os_water_simpl AS
SELECT ST_SimplifyPreserveTopology(ST_MakeValid(geom), 5.0) AS geom
FROM os_water;

CREATE INDEX os_water_simpl_gix ON os_water_simpl USING GIST (geom);
ANALYZE os_water_simpl;