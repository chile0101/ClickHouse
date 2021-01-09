events
events_mv_
events_mv_1
events_mv_2
kafka_events
kafka_events1
kafka_events2


drop table events_mv_;
drop table events_mv_1;
drop table events_mv_2;

drop table kafka_events1;
drop table kafka_events2;

show tables ;

show create events;
show create kafka_events;
show create kafka_events_mv;

show create table kafka_events_mv;
CREATE MATERIALIZED VIEW kafka_events_mv TO events
(
    `uuid` UUID,
    `event` String,
    `properties` String,
    `timestamp` DateTime64(6, 'UTC'),
    `team_id` Int64,
    `distinct_id` String,
    `elements_chain` String,
    `created_at` DateTime64(6, 'UTC'),
    `_timestamp` Nullable(DateTime),
    `_offset` UInt64
) AS
SELECT
    uuid,
    event,
    properties,
    timestamp,
    team_id,
    distinct_id,
    elements_chain,
    created_at,
    _timestamp,
    _offset
FROM kafka_events;

select * from kafka_events;

show tables ;

select * from events;
select * from kafka_events;

show create table kafka_events_mv;
show create table kafka_events;

