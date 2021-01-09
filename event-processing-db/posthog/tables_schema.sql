show tables ;

events
kafka_events
events_mv

person
kafka_person
person_mv

kafka_person_distinct_id
person_distinct_id
person_distinct_id_mv

kafka_session_recording_events
session_recording_events
session_recording_events_mv

events_with_array_props_view
infi_clickhouse_orm_migrations



---------------------------------EVENTS
show create table events;
CREATE TABLE posthog.events
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
SAMPLE BY uuid;


select * from events;
select count(*) from events;


show create table events_mv;
CREATE MATERIALIZED VIEW posthog.events_mv TO posthog.events
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
FROM posthog.kafka_events;


show create posthog.kafka_events;
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
SETTINGS kafka_broker_list = 'kafka', kafka_topic_list = 'clickhouse_events_proto', kafka_group_name = 'group1', kafka_format = 'Protobuf', kafka_schema = 'events:Event', kafka_skip_broken_messages = 100
;

select * from kafka_events;

----------------------PERSON
show create person;
CREATE TABLE posthog.person
(
    `id` UUID,
    `created_at` DateTime64(3),
    `team_id` Int64,
    `properties` String,
    `is_identified` Int8,
    `_timestamp` DateTime,
    `_offset` UInt64
);

select * from person order by _timestamp desc ;

show create table kafka_person;
CREATE TABLE posthog.kafka_person
(
    `id` UUID,
    `created_at` DateTime64(3),
    `team_id` Int64,
    `properties` String,
    `is_identified` Int8
)
ENGINE = Kafka('kafka', 'clickhouse_person', 'group1', 'JSONEachRow');


show create table person_mv;
CREATE MATERIALIZED VIEW posthog.person_mv TO posthog.person
(
    `id` UUID,
    `created_at` DateTime64(3),
    `team_id` Int64,
    `properties` String,
    `is_identified` Int8,
    `_timestamp` Nullable(DateTime),
    `_offset` UInt64
) AS
SELECT
    id,
    created_at,
    team_id,
    properties,
    is_identified,
    _timestamp,
    _offset
FROM posthog.kafka_person;


---------------------PERSION DISTINCT
--persion
01768495-83f9-0000-105b-a880f67a3be5
0176839f-8141-0000-1998-8051c3a7699a
0176839f-818e-0000-0e7a-59abdc80e42f



select * from person_distinct_id;
1768495767128f-0db1a1fe215b42-10201b0b-1fa400-17684957672ac3,    01768495-83f9-0000-105b-a880f67a3be5
176839f7dcfaf9-04f4da4b7b18ba-3a7e0a5e-1fa400-176839f7dd0992,    0176839f-8141-0000-1998-8051c3a7699a
a_irFOf6pVzCTesbsPLuZH6LtSKBCfndE56mvImDfA4,                     0176839f-8141-0000-1998-8051c3a7699a


----------------- OTHERS
show create table events_with_array_props_view;
select * from events_with_array_props_view;

['"Linux"','"Chrome"','"http://localhost:8000/persons"','"localhost:8000"','"/persons"','87','1080','1920','"web"','"1.8.0"','"2rvhvr5yaywhh5tv"','1608543371.845','"a_irFOf6pVzCTesbsPLuZH6LtSKBCfndE56mvImDfA4"','"176839f7dcfaf9-04f4da4b7b18ba-3a7e0a5e-1fa400-176839f7dd0992"','"http://localhost:8000/signup"','"localhost:8000"','"http://localhost:8000/insights?interval=day&display=ActionsLineGraph&events=%5B%7B%22id%22%3A%22%24pageview%22%2C%22name%22%3A%22%24pageview%22%2C%22type%22%3A%22events%22%2C%22order%22%3A0%7D%5D&properties=%5B%5D"','"localhost:8000"','[]','"a_irFOf6pVzCTesbsPLuZH6LtSKBCfndE56mvImDfA4"','"1.19.0"','false','"click"','1','"5Q8RDwzAFb9lLIVBjVrC6AGQBB8bPlDh6fMnhegj9wM"','"172.23.0.1"']


select * from infi_clickhouse_orm_migrations;

--------------------------


select * from events;
select count(*) from events;
select * from person;
select count(*) from person;

show tables ;

show create table kafka_person;
show create person;
show create table kafka_person_distinct_id;
show create person_mv;

select * from person;

select toUnixTimestamp('2020-11-01 00:00:00'),toUnixTimestamp('2021-01-01 00:00:00')



