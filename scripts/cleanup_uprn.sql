-- drop MVs that depend on stats before dropping the tables
DROP MATERIALIZED VIEW IF EXISTS target_cohorts;
DROP MATERIALIZED VIEW IF EXISTS parcel_features;

-- drop the previous stats/cluster/base tables if they exist
DROP TABLE IF EXISTS parcel_uprn_clusters;
DROP TABLE IF EXISTS parcel_uprn_stats;
DROP TABLE IF EXISTS parcel_uprn_base;