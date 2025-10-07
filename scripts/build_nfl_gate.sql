DROP MATERIALIZED VIEW IF EXISTS nfl_gate;

CREATE MATERIALIZED VIEW nfl_gate AS
SELECT parcel_id,
       BOOL_OR(is_nonland) AS is_nonland   -- if any source says TRUE, gate it out
FROM parcel_nonland_flags
GROUP BY parcel_id;

CREATE INDEX nfl_gate_pid_ix ON nfl_gate(parcel_id);
ANALYZE nfl_gate;
