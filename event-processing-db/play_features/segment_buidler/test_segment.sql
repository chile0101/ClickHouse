show databases ;
show tables ;
create database eventify1;

select count() from user_profile;

truncate table user_profile;
truncate table user_profile_final;

drop table user_profile;
CREATE TABLE user_profile
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `identity.keys` Array(String),
    `identity.vals` Array(String),
    `str_properties.keys` Array(String),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(String),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(String),
    `arr_properties.vals` Array(Array(String)),
    `at` DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at);

drop table user_profile_final;
CREATE TABLE user_profile_final
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `str_properties.keys` AggregateFunction(argMax, Array(String), DateTime64),
    `str_properties.vals` AggregateFunction(argMax, Array(String), DateTime64),
    `num_properties.keys` AggregateFunction(argMax, Array(String), DateTime64),
    `num_properties.vals` AggregateFunction(argMax, Array(Float64), DateTime64),
    `at_final` SimpleAggregateFunction(max, DateTime64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id);

drop table user_profile_final_mv;
CREATE MATERIALIZED VIEW user_profile_final_mv TO user_profile_final
AS
SELECT
    anonymous_id,
    tenant_id,
    argMaxState(str_properties.keys, at) AS `str_properties.keys`,
    argMaxState(str_properties.vals, at) AS `str_properties.vals`,
    argMaxState(num_properties.keys, at) AS `num_properties.keys`,
    argMaxState(num_properties.vals, at) AS `num_properties.vals`,
    max(at) AS at_final
FROM user_profile
GROUP BY
    tenant_id,
    anonymous_id;

-- insert 3 lan de co duplicate
insert into user_profile
select
    concat('a', toString(number)) as anonymous_id,
    1 as tenant_id,
    (select groupArray(concat('str_key_', toString(number))) from numbers(10)) as `str_properties.keys`,
    (select groupArray(concat('str_val_', toString(number))) from numbers(10)) as `str_properties.vals`,
    (select groupArray(concat('num_key_', toString(number))) from numbers(50)) as `num_properties.keys`,
    (select groupArray(rand()%1000) from numbers(50)) as `num_properties.vals`,
    toDateTime64('2020-01-01 00:00:00.000',3) + number * 60 AS at
from numbers(10000000)
;

---------TEST 1: select single profile (final) full properties.
SELECT *
FROM user_profile
WHERE tenant_id = 1
  and anonymous_id = 'a10';

SELECT anonymous_id,
       tenant_id,
       argMax(str_properties.keys, at),
       argMax(str_properties.vals, at),
       argMax(num_properties.keys, at),
       argMax(num_properties.vals, at),
       max(at)
FROM user_profile
WHERE tenant_id = 1
  and anonymous_id = 'a10134'
GROUP BY tenant_id,
         anonymous_id;


--- RESULT: (execution: 229 ms, fetching: 47 ms)


--------TEST 2: select pTrait for single profile


SELECT
       tenant_id,
       anonymous_id,
       num_pros_vals[indexOf(num_pros_keys, 'num_key_28')]
FROM
(
    SELECT
        anonymous_id,
        tenant_id,
        argMax(num_properties.keys, at) as num_pros_keys,
        argMax(num_properties.vals, at) as num_pros_vals,
        max(at)
    FROM user_profile
    WHERE tenant_id = 1 and anonymous_id = 'a202'
    GROUP BY
        tenant_id,
        anonymous_id
);



-- RESULT: (execution: 96 ms, fetching: 24 ms)

--------TEST 3: select n profiles for specifict profperties.
-- get a segment users
select  concat('a', toString(number))
from numbers(100000)
limit 500, 678;

SELECT
    anonymous_id,
    tenant_id,
    argMaxMerge(str_properties.keys) AS str_pros_keys,
    argMaxMerge(str_properties.vals) AS str_pros_vals,
    argMaxMerge(num_properties.keys) AS num_pros_keys,
    argMaxMerge(num_properties.vals) AS num_pros_vals,
    argMaxMerge(arr_properties.keys) AS arr_pros_keys,
    argMaxMerge(arr_properties.vals) AS arr_pros_vals,
    max(at_final) AS at
FROM user_profile_final
WHERE tenant_id = 1 and anonymous_id in (select  concat('a', toString(number))
                                        from numbers(100000)
                                        limit 500, 678)
GROUP BY
    tenant_id,
    anonymous_id;

-- RESULT : (execution: 963 ms, fetching: 52 ms)


--------TEST 4: select n properties for single profile

SELECT
       num_properties.vals
SELECT
    anonymous_id,
    tenant_id,
    argMaxMerge(str_properties.keys) AS str_pros_keys,
    argMaxMerge(str_properties.vals) AS str_pros_vals,
    argMaxMerge(num_properties.keys) AS num_pros_keys,
    argMaxMerge(num_properties.vals) AS num_pros_vals,
    argMaxMerge(arr_properties.keys) AS arr_pros_keys,
    argMaxMerge(arr_properties.vals) AS arr_pros_vals,
    max(at_final) AS at
FROM user_profile_final
WHERE tenant_id = 1 and anonymous_id  = 'a12345'
GROUP BY
    tenant_id,
    anonymous_id;









---
select * from user_profile_final where anonymous_id = 'a202';


create table test_1(
    id String,
    name String,
    name_1 Nullable(String)
)
ENGINE = Log()
;

insert into test_1 values ('a1', 'chie', 'chi');
insert into test_1 values ('a1', 'chie', '');
insert into test_1 values ('a1', 'chie', NULL);

select * from test_1;


show tables;

select count(*) from user_profile;

select * from user_profile limit 100;


select * from user_profile_num_rw limit 10;



CREATE DATABASE segment_test;

drop table events;

CREATE TABLE events
(
    `id` String,
    `tenant_id` UInt16,
    `anonymous_id` String,
    `event_name` LowCardinality(String),
    `str_properties.keys` Array(LowCardinality(String)),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(LowCardinality(String)),
    `num_properties.vals` Array(Float64),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, anonymous_id, event_name, at)
SETTINGS index_granularity = 8192;



-- INSERT DATA
create table pros(
    str_keys String,
    str_vals String
)ENGINE = Log;

insert into pros
with arrayMap(x -> concat('event_',toString(x)), range(1,21)) AS event_names
select








select arrayMap(x -> concat('str_prop_',toString(x),'_val_',toString(x)), range(1,21)) AS str_prop_vals;
select     arrayMap(x -> concat('str_prop_',toString(x),'_val_',toString(rand(x) % 10 )), range(1,21)) AS str_prop_vals;





INSERT INTO events
WITH
    arrayMap(x -> concat('event_',toString(x)), range(1,21)) AS event_names,
    arrayMap(x -> concat('str_prop_',toString(x)), range(1,21)) AS str_prop_keys,
    arrayMap(x -> concat('num_prop_',toString(x)), range(1,21)) AS num_prop_keys
SELECT
    randomPrintableASCII(length('DEJ9BsR9C1rEm1yjI9B9ANhJsCq')) as id,
    1 as tenant_id,
    concat('a_', toString(number)) as anonymous_id,
    event_names[rand(4) % length(event_names) + 1] as event_name,
    str_prop_keys as `str_properties.keys`,
    arrayMap(x -> concat(x,'_val_', toString(rand(x) % 20)), `str_properties.keys`) AS `str_properties.vals`,
    num_prop_keys as `num_properties.keys`,
    arrayMap(x ->  rand(x) % 1000000, `num_properties.keys`) AS `num_properties.vals`,
    toDateTime('2020-11-01 00:00:00') + rand(number) % (3600 * 24 * 60) as at
FROM numbers(100000)
;





SELECT sequenceCount('(?1)(?1)(?1)(?t<=2600)(?2)(?t<100)()')(at, number = 1, number = 5) FROM test;




select * from  events limit 12;



select anonymous_id,event_name, count(event_name) as count from events
group by anonymous_id, event_name
order by  anonymous_id ,count desc ;


select * from events where anonymous_id = 'a_12345';

select * from (
SELECT anonymous_id,
       sequenceCount('(?1)(?1)(?t<=5000000)(?2)')(at, event_name = 'event_11', event_name = 'event_4') as count FROM events
group by  anonymous_id having count > 0 ) where anonymous_id = 'a_0';










select count(*) from events

select 7 * 86400;


select count(*) from events ;

select count distinct (anonymous_id) from events;


select * from events where anonymous_id = 'a_2222';


select count(*) from (
                    SELECT anonymous_id,
                    sequenceCount('(?1)(?1)(?t<=1296000)(?2)(?2)(?t<=1296000)(?3)(?3)(?3)')
                        (at, event_name = 'event_10', event_name = 'event_12', event_name = 'event_14') as count
                    FROM events
                    group by  anonymous_id having count > 0
                    );


select count(*) from (
                    SELECT anonymous_id,
                    sequenceCount('(?1)(?1)(?t<=1296000)(?2)(?2)(?t>604800)(?3)(?3)(?3)')
                        (at, event_name = 'event_11', event_name = 'event_14', event_name = 'event_19') as count
                    FROM events
                    group by  anonymous_id having count > 0
                    );


select * from (
                    SELECT anonymous_id,
                    sequenceCount('(?1)(?1)(?t<=1296000)(?2)(?2)(?t>604800)(?3)(?3)(?3)')
                        (at, event_name = 'event_11', event_name = 'event_14', event_name = 'event_19') as count
                    FROM events
                    group by  anonymous_id having count > 0
                    ) limit 2;



select toStartOfDay(at) as day , anonymous_id, event_name, count(event_name) from events where anonymous_id = 'a_1111'
group by anonymous_id, toStartOfDay(at), event_name
order by day
;

select * from (
                    SELECT anonymous_id,
                    sequenceCount('(?1)(?1)(?1)(?t<=60400800)(?2)(?2)(?t<60004800)(?3)(?3)')
                        (at, event_name = 'event_10', event_name = 'event_12', event_name = 'event_14') as count
                    FROM events
                    group by  anonymous_id having count > 0
                    ) where anonymous_id = 'a_2222';


-------------------------test dung dan
show tables;


drop table test;
create table test(
    user String,
    time UInt16,
    number UInt16
)
ENGINE = Log()
;

insert into test values
('u1',1,1),('u1', 3, 2),

('u2',5,3),('u2', 6, 2),('u2', 7, 3),
('u3', 10,2);

insert into test values ('u2',4,1);
insert into test values ('u3',11,1);
insert into test values ('u4',15,1);
insert into test values ('u4',16,2);
insert into test values ('u4',17,3);
insert into test values ('u4',18,4);

insert into test values ('u1',4,4);

insert into test values ('u4',20,1);
insert into test values ('u4',22,2);
insert into test values ('u4',24,3);

-----------------------------------
SELECT * FROM test order by time;



-- thuc hien 1 2 lan


select user,
       sequenceCount('(?1).*(?2)')(time, number=1, number=2)
from test
group by user;

--------------

with (select 1) as one
select user,
       sequenceCount('(?1)(?2)(?3)')(time, number=1, number=2,number=3)
from test
group by user;



