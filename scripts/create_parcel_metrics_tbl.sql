SET statement_timeout = 0;
SET lock_timeout = '5s';

DROP TABLE IF EXISTS parcel_metrics_tbl;

-- UNLOGGED for faster rebuilds; safe because we recreate it
CREATE UNLOGGED TABLE parcel_metrics_tbl (
  parcel_id           bigint PRIMARY KEY,
  acres               numeric(12,2),
  land_ratio          numeric(6,4),
  water_ratio         numeric(6,4),
  road_ratio          numeric(6,4),
  rail_ratio          numeric(6,4),
  water_pct           numeric(5,1),
  land_pct            numeric(5,1),
  compactness         numeric(6,4),
  is_offshore         boolean,
  is_road_corridor    boolean,
  is_rail_corridor    boolean,
  is_roadlike_longthin boolean
);

CREATE INDEX parcel_metrics_tbl_pid_ix ON parcel_metrics_tbl (parcel_id);
ANALYZE parcel_metrics_tbl;