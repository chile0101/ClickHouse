create table events
(
    visitor_id String,
    date       Date,
    event_type String,
    product_id String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (visitor_id, date);

insert into events
values ('001', '2017-01-01', 'click', 'iphone7'),
       ('001', '2017-01-01', 'click', 'iphone7'),
       ('001', '2017-01-01', 'click', 'iphone7'),
       ('002', '2017-01-02', 'click', 'iphone7'),
       ('003', '2017-01-02', 'click', 'iphone7'),
       ('004', '2017-01-02', 'click', 'iphone8')


-- Problem
-- 0.011
    SELECT uniq(visitor_id) AS c
FROM events
WHERE event_type = 'click'
  AND product_id = 'iphone7'
  AND date >= '2017-01-01'
  AND date <= '2017-01-02'

-- 0.948 sec
------------------------------------------SOLUTION-------------------------------
create table events_unique(
    date Date,
    event_type String,
    product_id String,
    value AggregateFunction (uniq, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (event_type, product_id, date)

insert into events_unique



insert into events_unique
select date, event_type, product_id, uniqState(visitor_id) as value
from events
group by date, event_type, product_id


--------------------or use MV

CREATE MATERIALIZED VIEW events_unique_mv
    TO events_unique
AS
SELECT date,
       event_type,
       product_id,
       uniqState(visitor_id) AS value
FROM events
GROUP BY date, event_type, product_id


-- solution 

SELECT uniqMerge(value)
FROM events_unique
WHERE event_type = 'click'
  AND product_id = 'iphone7'
  AND date >= '2017-01-01'
  AND date <= '2017-01-02'

INSERT INTO events
values ('005', '2017-01-01', 'click', 'iphone7');


-----------------------------------------------------------------------------------------------
select
    avg(x),
    uniq(x)
from (
    select 123 as x
    union all
    select 456
);


select
    toTypeName(avgState(x)),
    toTypeName(uniqState(x))
from (
    select 123 as x
    union all
    select 456
)
FORMAT Vertical

------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_profile
(
    tenant_id    UInt16,
    user_id      String,
    anonymous_id String,

    str_properties Nested(
        keys String,
        vals String
    ),
    created_at   DateTime

) Engine MergeTree()
Order By (tenant_id, anonymous_id, created_at);


CREATE TABLE IF NOT EXISTS user_profile_final
(
    tenant_id             UInt16,
    user_id               AggregateFunction(argMax, String, DateTime),
    anonymous_id          String,

    "str_properties.keys" AggregateFunction(argMax, Array(String), DateTime),
    "str_properties.vals" AggregateFunction(argMax, Array(String), DateTime),

    created_at_final            SimpleAggregateFunction(max, DateTime)
)
Engine = AggregatingMergeTree()
Order by (tenant_id, anonymous_id);


CREATE MATERIALIZED VIEW IF NOT EXISTS user_profile_final_mv
    to user_profile_final
as
select tenant_id,
       argMaxState(user_id, created_at)             as user_id,
       anonymous_id,

       argMaxState(str_properties.keys, created_at) as "str_properties.keys",
       argMaxState(str_properties.vals, created_at) as "str_properties.vals",

       max(created_at)                              as created_at_final
from user_profile
group by tenant_id, anonymous_id;


insert into user_profile values
(1, 'u1', 'a1', ['gender','location'], ['male','hcm'], now());

insert into user_profile values
(1, 'u1', 'a1', ['gender','location'], ['female','danang'], now());

insert into user_profile values
(1, 'u1', '100', ['gender','location'], ['aaa','bbb'], '2023-01-01 00:00:00');

insert into user_profile values
(1, 'u1', 'a2', ['gender','location'], ['other','quangnam'], now());

insert into user_profile
select rand(1)%3, 'u', rand()%1000, ['gender','location'],['xxx','hcm'], now() + number*60
from system.numbers
limit 1000000;

insert into user_profile_final
select tenant_id,
       argMaxState(user_id, created_at)             as user_id,
       anonymous_id,

       argMaxState(str_properties.keys, created_at) as "str_properties.keys",
       argMaxState(str_properties.vals, created_at) as "str_properties.vals",

       max(created_at)                              as created_at_final
from user_profile
group by tenant_id, anonymous_id;

select * from user_profile where anonymous_id = '100' and tenant_id = 1;
select * from user_profile_final where anonymous_id = '100';
select
    tenant_id,
    argMaxMerge(user_id),
    anonymous_id,
    argMaxMerge(str_properties.keys),
    argMaxMerge(str_properties.vals),
    max(created_at_final)
from user_profile_final
group by tenant_id, anonymous_id;

select count() from user_profile_final final;
select count() from user_profile;

optimize table user_profile_final;


select * from
(
select
    tenant_id,
    argMaxMerge(user_id),
    anonymous_id,
    argMaxMerge(str_properties.keys),
    argMaxMerge(str_properties.vals),
    max(created_at_final)
from user_profile_final
group by tenant_id, anonymous_id
    )
    where tenant_id = 1 and anonymous_id = '100';




--------------------------------------------- Bai toan. dem so luong order theo ngay, tuan, thang
create table orders(
    order_id Int32,
    user_id UInt32,
    created_at DateTime
)ENGINE = MergeTree()
partition by toYYYYMM(created_at)
order by order_id;

----- c1
create table order_counting_daily(
    day Date,
    order_count AggregateFunction(count, Int32)
)ENGINE = AggregatingMergeTree()
order by (day);


create materialized view order_daily_mv to order_counting_daily
as
select toDate(created_at) as day,
       countState(order_id) as order_count
from orders
group by day;

insert into orders values
(1, 1, now());

insert into orders values
(2, 1, now());

insert into orders values
(3, 1, now());

insert into orders values
(4, 2, now());

insert into orders
select number,
       rand()%1000,
       now() + number*60
from system.numbers
limit 1000000;


select day,
       countMerge(order_count) as orders
from order_counting_daily
where day = '2022-05-31'
group by day;
-- 0.003s

select toDate(created_at) as day,
       count(order_id) as orderss
from orders
where day = '2022-05-31'
group by day;
-- 0.035

---------------------- Trong mot ngay 1 user co may giao dich, total_value, max, min, avg.
create table events(
    event_id UInt32,
    event_type String,
    user_id UInt32,
    value UInt32,
    created_at DateTime
)ENGINE = MergeTree()
partition by toYYYYMM(created_at)
order by (event_id, event_type, user_id);

-- target
create table event_agg_daily(
    day Date,
    user_id UInt32,
    total_event AggregateFunction(count, Int32),
    total_value AggregateFunction(sum, Int32),
    max_value AggregateFunction(max, Int32),
    min_value AggregateFunction(min, Int32),
    avg_value AggregateFunction(avg, Float32)
)ENGINE = AggregatingMergeTree()
ORDER BY day, user_id;

create materialized view event_agg_daily_mv to event_agg_daily as
select
    toDate(created_at) as day,
