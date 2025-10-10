DROP MATERIALIZED VIEW IF EXISTS roads_buf;
CREATE MATERIALIZED VIEW roads_buf AS
SELECT ST_MakeValid(ST_Buffer(geom, 15.0)) AS geom FROM os_roads_national
UNION ALL
SELECT ST_MakeValid(ST_Buffer(geom, 12.0)) AS geom FROM os_roads_regional
UNION ALL
SELECT ST_MakeValid(ST_Buffer(geom,  9.0)) AS geom FROM os_roads_local;
CREATE INDEX roads_buf_gix ON roads_buf USING GIST (geom);
ANALYZE roads_buf;

DROP MATERIALIZED VIEW IF EXISTS rail_buf;
CREATE MATERIALIZED VIEW rail_buf AS
SELECT ST_MakeValid(ST_Buffer(geom, 10.0)) AS geom FROM os_rail;
CREATE INDEX rail_buf_gix ON rail_buf USING GIST (geom);
ANALYZE rail_buf;