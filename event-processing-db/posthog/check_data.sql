select count(*) from events;

01768d8c-8507-0001-b495-44821c463492;


show tables ;

events
events_mv_
events_mv_1
events_mv_2
kafka_events
kafka_events1
kafka_events2



events_properties_view
events_with_array_props_mv
events_with_array_props_view
infi_clickhouse_orm_migrations

kafka_person
kafka_person_distinct_id
kafka_session_recording_events
person
person_distinct_id
person_distinct_id_mv
person_mv
session_recording_events
session_recording_events_mv;

select * from kafka_events;
show create table kafka_events;

CREATE TABLE posthog.kafka_events
(
    `uuid` UUID,
    `event` String,
    `properties` String,
    `timestamp` DateTime64(6, 'UTC'),
    `team_id` Int64,
    `distinct_id` String,
    `elements_chain` String,
    `created_at` DateTime64(6, 'UTC')
)
ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka',
    kafka_topic_list = 'clickhouse_events_proto',
    kafka_group_name = 'group1',
    kafka_format = 'Protobuf',
    kafka_schema = 'events:Event',
    kafka_skip_broken_messages = 100
;


---------test
show create test_kafka_event;
drop table test_kafka_event;
CREATE TABLE test_kafka_event
(
    `uuid` UUID,
    `event` String,
    `properties` String,
    `timestamp` DateTime64(6, 'UTC'),
    `team_id` Int64,
    `distinct_id` String,
    `elements_chain` String,
    `created_at` DateTime64(6, 'UTC')
)
ENGINE = Kafka()
SETTINGS kafka_broker_list = 'kafka',
    kafka_topic_list = 'events_write_ahead_log',
    kafka_group_name = 'group1',
    kafka_format = 'JSONEachRow',
    kafka_skip_broken_messages = 100
;
    kafka_schema = 'events:Event',

drop  table test_kafka_event_mv;
show create table test_kafka_event_mv;
CREATE MATERIALIZED VIEW test_kafka_event_mv TO test_events
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
FROM test_kafka_event
    ;

show create table test_events;
CREATE TABLE test_events
(
    `uuid` UUID,
    `event` String,
    `properties` String,
    `timestamp` DateTime64(6, 'UTC'),
    `team_id` Int64,
    `distinct_id` String,
    `elements_chain` String,
    `created_at` DateTime64(6, 'UTC'),
    `_timestamp` DateTime,
    `_offset` UInt64
)
ENGINE = ReplacingMergeTree(_timestamp)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, toDate(timestamp), distinct_id, uuid)
SAMPLE BY uuid
;


select * from test_events limit 10;
select * from kafka_person;
show create table kafka_person;
select * from test_kafka_event;

select * events
;


------test protobuf
SELECT * FROM test_kafka_event.table FORMAT Protobuf SETTINGS format_schema = 'events:Event';

select * from