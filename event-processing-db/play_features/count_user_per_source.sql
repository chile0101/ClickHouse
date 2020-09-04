select * from events;


CREATE TABLE IF NOT EXISTS events
(
    `tenant_id` UInt16,
    `user_id` String,
    `anonymous_id` String,
    `event_name` String,
    `str_properties.keys` Array(String),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(String),
    `num_properties.vals` Array(Float32),
    `arr_properties.keys` Array(String),
    `arr_properties.vals` Array(Array(String)),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, anonymous_id, event_name, at)
;



insert into events
    with
        ['Added to Card','App Launched','App Uninstalled','Category Viewed','Order Completed','Payment Offer Applied', 'Product Viewed', 'Searched'] as event_names,
        ['web','mobile'] as sources
    select
        rand(1)%1+1 as tenant_id,
        'u' as user_id,
        concat('a', toString(number)) as anonymous_id,
        event_names[rand(2)%length(event_names)+1] as event_name,
        ['source.scope'] as `str_properties.keys`,
        [sources[rand(3)%length(sources)+1]] as `str_properties.vals`,
        [] as `num_properties.keys`,
        [] as `num_properties.vals`,
        [''] as `arr_properties.keys`,
        [[]] as `arr_properties.vals`,
        toDateTime('2017-02-28 00:00:10')+number*3600 as at
    from system.numbers
    limit 100
;

select * from events;
insert into events values
(1,'user', 'a1', 'order', ['source.scope'],['fb'],[],[],[],[],yesterday()),
(1,'user', 'a1', 'checkout', ['source.scope'],['fb'],[],[],[],[],yesterday()),
 (1,'user', 'a1', 'view', ['source.scope'],['web'],[],[],[],[],yesterday()),
(1,'user', 'a2', 'checkout', ['source.scope'],['web'],[],[],[],[],yesterday())
;

insert into events values
(1,'user', 'a2', 'checkout', ['source.scope'],['fb'],[],[],[],[],now());


insert into events values
(1,'user', 'a2', 'view', ['source.scope'],['fb'],[],[],[],[],'2020-08-26 00:01:20');
----------------------------------------------------- UNIQ
create table if not exists users_source_agg_daily(
    day Date,
    tenant_id UInt16,
    source_id String,
    value AggregateFunction(uniq, String)
)ENGINE MergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, source_id, day)
;

create materialized view if not exists users_source_agg_daily_mv to users_source_agg_daily
as
select toDate(at) as day,
       tenant_id,
       str_properties.vals[indexOf(str_properties.keys, 'source.scope')] as source_id,
       uniqState(anonymous_id) as value
from events
group by day, tenant_id, source_id
;


date -> month_of_the_year(date) -> month
date -> week_of_the_year(date) -> week
date -> year(date) -> year

sql_builder(group_by)-> String:
    sql = f""
    if group_by = "date":
        time = "day"
    if group_by = "month":
        time = "toMonth(day)"
    if group_by = "week":
        time =  "toWeek(day)"
    if group_by = "year":
        time = "toYear(day)"
    return sql.format(time)

----------------------aggregate by day
select * from events;

SELECT
    day,
    tenant_id,
    source_id,
    uniqMerge(value) AS user_count
FROM users_source_agg_daily
WHERE tenant_id = 1
  and source_id = 'fb'
  and day between '2020-08-25' and '2020-08-26'
GROUP BY
    day,
    tenant_id,
    source_id
ORDER BY
    day ASC,
    tenant_id ASC,
    source_id ASC
;

select * from users_source_agg_daily_v;

-----------------------agregate by week
select * from events;
select toStartOfMonth(day) as m,
       tenant_id,
       source_id,
       uniqMerge(value)
from users_source_agg_daily
group by m, tenant_id, source_id;

-----------------------aggregate by year
select toStartOfYear(day) as y,
       tenant_id,
       source_id,
       uniqMerge(value)
from users_source_agg_daily
group by y, tenant_id, source_id;



-------------------------------------------------------count events name per source
create table if not exists event_name_source_agg_daily(
    day Date,
    tenant_id UInt16,
    event_name String,
    source_id String,
    count UInt16
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, source_id, event_name, day)
;

create materialized view event_name_source_agg_daily_mv to event_name_source_agg_daily
as
select
    toDate(at) as day,
    tenant_id,
    event_name,
    str_properties.vals[indexOf(str_properties.keys, 'source.scope')] as source_id,
    count() as count
from events
group by (day, tenant_id, event_name, source_id)
;
---------------- aggregate by day
select day,
       tenant_id,
       event_name,
       source_id,
       sum(count)
from event_name_source_agg_daily
group by day, tenant_id, event_name, source_id
order by day, tenant_id, event_name, source_id;

-----------------aggregate by month
select toStartOfMonth(day) as month,
       tenant_id,
       source_id,
       event_name,
       sum(count)
from event_name_source_agg_daily
group by day, tenant_id, event_name, source_id
order by day, tenant_id, event_name, source_id;



