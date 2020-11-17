select count(*) from user_profile_final_v;
truncate table eventify_demo.user_profile;
truncate table eventify_demo.user_profile_final;

------------------ 100k profile
insert into user_profile
    with
    ['Nam', 'Vu', 'Long', 'Thien'] as first_names,
    ['Nguyen Van', 'Hoang', 'Le Huu'] as last_names,
    ['Male','Female'] as genders,
    ['Hochiminh','Hanoi','Danang', 'Vungtau','Cantho'] as locations,
    [['facebook', 'google'],[ 'onesignal'], ['google','mail' ||
                                                      'chimp'],['facebook']] as channels
    select
           concat('a', toString(number)) as anonymous_id,
           rand(1)%1+1 as tenant_id,
           ['user_id', 'email','email'] as `identity.keys`,
           [concat('user-',randomPrintableASCII(5)), concat(randomPrintableASCII(5),'@gmail.com'), 'chile@gmail.com'] as `identity.vals`,
           ['first_name','last_name','gender','location_city', 'location_country'] as `str_properties.keys`,
           [first_names[rand(6)%length(first_names) + 1],last_names[rand(7)%length(last_names) + 1],genders[rand(2)%length(genders) + 1], locations[rand(3)%length(locations)+1],'Viet Nam' ] as `str_properties.vals`,
           ['first_activity','last_activity', 'days_since_last_order', 'total_revenue', 'total_orders','avg_order_value'] as `num_properties.keys`,
           [toUnixTimestamp(yesterday()+number),toUnixTimestamp(now()+number), toUnixTimestamp('2016-01-01 00:00:00')+number*60, rand(5)%300, rand(5)%100, rand(5)%100] as `num_properties.vals`,
           ['channels','arr_test_key'],
           [channels[rand(10)%length(channels) + 1],['arr_test_val']],
           now()
    from system.numbers
    limit 10
;


insert into user_profile
select
    concat('a', toString(number)) as anonymous_id,
    1 as tenant_id,
    ['email'] as `identity.keys`,
    [randomPrintableASCII(10)] as `identity.vals`,
    (select groupArray(concat('str_key_', toString(number))) from numbers(10)) as `str_properties.keys`,
    (select groupArray(concat('str_val_', toString(number))) from numbers(10)) as `str_properties.vals`,
    (select groupArray(concat('num_key_', toString(number))) from numbers(50)) as `num_properties.keys`,
    (select groupArray(rand()%1000) from numbers(50)) as `num_properties.vals`,
    ['channels'] as `arr_properties.keys`,
    [['arr_val_test']] as `arr_properties.vals`,
    toDateTime64('2020-01-01 00:00:00.000',3) + number * 60 AS at
from numbers(100000)
;

select groupArray(concat('str_pros_', toString(number))) from numbers(10);




------------------------------------------------------------------------------------------------TEST NEW TABLE
show tables;
user_profile
user_profile_num
truncate table user_profile_num_rw;
user_profile_string
truncate table user_profile_string_rw;

-- create old table
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

-- insert to old table
insert into user_profile
select
    randomPrintableASCII(26) as anonymous_id,
    1 as tenant_id,
    ['email'] as `identity.keys`,
    [randomPrintableASCII(10)] as `identity.vals`,
    (select groupArray(concat('str_key_', toString(number))) from numbers(10)) as `str_properties.keys`,
    (select groupArray(concat('str_val_', toString(number))) from numbers(10)) as `str_properties.vals`,
    (select groupArray(concat('num_key_', toString(number))) from numbers(50)) as `num_properties.keys`,
    (select groupArray(rand()%1000) from numbers(50)) as `num_properties.vals`,
    ['channels'] as `arr_properties.keys`,
    [['arr_val_test']] as `arr_properties.vals`,
    toDateTime64('2020-01-01 00:00:00.000',3) + number * 60 AS at
from numbers(1000000)
;


select toDateTime64('2020-01-01 00:00:00.000',3) + number * 60 AS at
from numbers(10);



show tables;
show create table user_profile_num_rw;
CREATE TABLE eventify2.user_profile_num_rw
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `prop_key` LowCardinality(String),
    `prop_val` Float64,
    `at` DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, prop_key, at);
select count() from user_profile_num_rw;
select * from user_profile_num_rw limit 10;






select * from events_campaign order by created_at desc ;
select * from events_campaign where anonymous_id = 'DDgIhUR2Wxotdzu8QZowHyjFpIE';





select tenant_id, anonymous_id, `str_properties.vals` from events where anonymous_id = 'DDf1aRTlVhEgx141yw4uhvxUrLc';

--------------------------------------------------METRIC PER ROW.
show tables ;


---- create new tables
CREATE TABLE profile_str
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `str_key` LowCardinality(String),
    `str_val` String,
    `at` DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, str_key, at);


CREATE TABLE profile_num
(
    anonymous_id String,
    tenant_id UInt16,
    num_key LowCardinality(String),
    num_val Float64,
    at DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, num_key, at);


insert into profile_num
select anonymous_id, tenant_id,str_key, str_val, at from user_profile array join
 `num_properties.keys` as str_key,
`num_properties.vals` as str_val;


insert into profile_str
select anonymous_id, tenant_id,str_key, str_val, at from user_profile array join
 `str_properties.keys` as str_key,
`str_properties.vals` as str_val;

drop table profile_num;
drop table profile_str;

select count(*) from user_profile;
--6418

select count(*) from profile_num;
--42616

select count(*) from profile_str;
--180510




souce_id , target_id
type: anonymous,
value: 123567

type: email,
value: chile@primedata.ai

CREATE TABLE eventify.user_profile
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
    `at` DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at)
;

select tenant_id, anonymous_id, str_properties.keys, str_properties.vals from user_profile;
['firstVisit',
'lastVisit',
'systems.goals.1js2dYzHxpzsYDXOrgCVLFQuEJR_0_0StartReached',
'systems.goals.1js3CPNZzSelNVm78GG2E8Aksev_0_0StartReached',
'systems.goals.1js1xIzEc03e9XIdm34Iw9u1liz_0_0StartReached',
'systems.lastUpdated']

