show tables;


show create table segment_user;
-- CREATE TABLE eventify_stag.segment_user
-- (
--     `segment_id` String,
--     `tenant_id` UInt16,
--     `user` String,
--     `status` UInt8,
--     `at` DateTime64(3)
-- )
-- ENGINE = MergeTree()
-- PARTITION BY toYYYYMM(at)
-- ORDER BY (tenant_id, segment_id, user, at)

select *
from segment_user
;



-- live, historical
drop table segment_user_historical_queue;
create table segment_user_historical_queue(
    segment_id String,
    tenant_id UInt16,
    user String,
    status UInt8,
    at DateTime64(3)
)ENGINE = Kafka()
SETTINGS
    kafka_broker_list =  '10.110.1.224:9092, 10.110.1.224:9093,10.110.1.224:9094',
    kafka_topic_list = 'segment-user-historical-update',
    kafka_group_name = 'segment_user_historical_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;
;


show create table segment_user_historical_queue;

insert into segment_user_historical_queue values ('s1', 1, 'u1', now64());


INSERT INTO segment_user VALUES (tenant_id, segment_id, user, status, at)
AS SELECT tenant_id,
        anonymous_id,
        's1' AS segment_id,
        1 AS status,
        now64() AS at
FROM ({nested_query});

select * from segment_user;
INSERT INTO segment_user (segment_id, tenant_id, user, status, at)
SELECT
        's1' AS segment_id,
        tenant_id,
        anonymous_id as user,
        1 AS status,
        now64() AS at
FROM (
      select
        1 as tenant_id,
        concat('a', toString(number)) as anonymous_id
        from numbers(1000000)
     );



select * from segment_user where tenant_id = 11;

select count() from segment_user; -- 35470 --1035470;


s
