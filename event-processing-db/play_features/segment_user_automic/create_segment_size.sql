show tables;
drop table segment_size;
CREATE TABLE segment_size
(
    `segment_id` String,
    `tenant_id` UInt16,
    `time_stamp` DateTime,
    `total_user` UInt32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
ORDER BY (tenant_id, segment_id,time_stamp)
;

-- drop table segment_size_final;
-- CREATE TABLE segment_size_final
-- (
--     `segment_id` String,
--     `tenant_id` UInt16,
--     `time_stamp` DateTime,
--     `total_user` AggregateFunction(argMax, UInt32, DateTime64)
-- )
-- ENGINE = MergeTree()
-- PARTITION BY toYYYYMM(time_stamp)
-- ORDER BY (tenant_id, segment_id,time_stamp)
-- ;
-- drop table segment_size_final_mv;
-- CREATE MATERIALIZED VIEW segment_size_final_mv TO segment_size_final AS
-- SELECT segment_id,
--        tenant_id,
--        toDateTime(segment_size.time_stamp) AS time_stamp,
--        argMaxState(total_user, segment_size.time_stamp) AS total_user
-- FROM segment_size
-- GROUP BY tenant_id, segment_id, time_stamp
-- ORDER BY tenant_id, segment_id, time_stamp
-- ;
--
-- drop table segment_size_final_v;
-- CREATE VIEW segment_size_final_v AS
-- SELECT segment_id,
--        tenant_id,
--        time_stamp,
--        argMaxMerge(total_user) as total_user
-- FROM segment_size_final
-- GROUP BY tenant_id, segment_id, time_stamp
;


