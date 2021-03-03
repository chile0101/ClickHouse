


;



CREATE TABLE events_total_distinct_users_by_source_scope_ts
(
    `tenant_id` UInt16,
    `source_scope` String,
    `time_stamp` DateTime,
    `value` AggregateFunction(uniq, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
ORDER BY (tenant_id, source_scope, time_stamp)
;

drop table events;
drop table profile_v;

drop table events_campaign;
drop table events_conversion;


show tables ;

select * from profile_num;

