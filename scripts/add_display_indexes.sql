-- Indexes for fast filtering/paging on the display table
CREATE INDEX IF NOT EXISTS cohort_parcels_map_acres_ix            ON cohort_parcels_map (acres);
CREATE INDEX IF NOT EXISTS cohort_parcels_map_uprn_total_ix       ON cohort_parcels_map (uprn_count);
CREATE INDEX IF NOT EXISTS cohort_parcels_map_uprn_interior_ix    ON cohort_parcels_map (uprn_interior_count);

-- For nested UPRN lookups by parcel
CREATE INDEX IF NOT EXISTS parcel_uprn_base_pid_ix ON parcel_uprn_base (parcel_id);
-- If you built parcel_interior earlier, keep its parcel_id index too:
CREATE INDEX IF NOT EXISTS parcel_interior_pid_ix  ON parcel_interior (parcel_id);
