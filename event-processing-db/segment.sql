create table segment_agg(
    segment_id: String,
    time_stamp: Datetime,   --2020-07-22 00:00:00
    metricname: string
    metric_aggs: Nested(
            keys: String
            vals: Float32
    )
)



--------------------------------------------segment-hello-world--------------------------------------

create table segments(
    segment_id String,
    users Array(String)
) Engine = Log()

insert into segments values
('s1',['a1','a2','a4']),
('s2',['a2','a3'])

create table users(
    user_id String,
    value Int32
) Engine = Log()

insert into users values
('a1', 1),
('a2', 2),
('a3', 3),
('a4', 4),
('a5', 5)

create table segment_agg(
    segment_id String,
    time_stamp DateTime,
    metric_name String,
    metrics_agg Nested(
        keys String,
        vals Float32
    )
) Engine = MergeTree()
order by (segment_id, metric_name, time_stamp)


create materialized view segment_agg_mv
to segment_agg
as  select
        segment_id,
        toStartOfDay(now()) as time_stamp,
        'age' as metric_name,
        ['sum'] as "metrics_agg.keys" ,
        [sum(value)] as "metrics_agg.vals"
    from segments array join users
    LEFT join users
    on segments.users = users.user_id
    group by segment_id, time_stamp


-- insert them s3, s4, xem co vao segment_agg ko ?
insert into segments values
('s3',['a8','a9']),
('s4',['a2','a3'])


-- insert a8, a9, vao user, segment_agg co duoc update hay ko ?
insert into users values
('a8', 8),('a9', 9)
--> segment_agg ko update -> mv not work khi insert vao table users.


-- ket qua segment_agg hien tai, thu insert them segment_id = s4, s3 xem sao.
SELECT *
FROM segment_agg

┌─segment_id─┬──────────time_stamp─┬─metric_name─┬─metrics_agg.keys─┬─metrics_agg.vals─┐
│ s5         │ 2020-07-22 00:00:00 │ age         │ ['sum']          │ [2]              │
└────────────┴─────────────────────┴─────────────┴──────────────────┴──────────────────┘
┌─segment_id─┬──────────time_stamp─┬─metric_name─┬─metrics_agg.keys─┬─metrics_agg.vals─┐
│ s4         │ 2020-07-22 00:00:00 │ age         │ ['sum']          │ [7]              │
└────────────┴─────────────────────┴─────────────┴──────────────────┴──────────────────┘

insert into segments values
('s3',['a8','a9']),
('s4',['a2','a3'])

┌─segment_id─┬──────────time_stamp─┬─metric_name─┬─metrics_agg.keys─┬─metrics_agg.vals─┐
│ s5         │ 2020-07-22 00:00:00 │ age         │ ['sum']          │ [2]              │
└────────────┴─────────────────────┴─────────────┴──────────────────┴──────────────────┘
┌─segment_id─┬──────────time_stamp─┬─metric_name─┬─metrics_agg.keys─┬─metrics_agg.vals─┐
│ s4         │ 2020-07-22 00:00:00 │ age         │ ['sum']          │ [7]              │
└────────────┴─────────────────────┴─────────────┴──────────────────┴──────────────────┘
┌─segment_id─┬──────────time_stamp─┬─metric_name─┬─metrics_agg.keys─┬─metrics_agg.vals─┐
│ s3         │ 2020-07-22 00:00:00 │ age         │ ['sum']          │ [5]              │
│ s4         │ 2020-07-22 00:00:00 │ age         │ ['sum']          │ [7]              │
└────────────┴─────────────────────┴─────────────┴──────────────────┴──────────────────┘


-- Lam sao update user cung update segment_agg ??

-----------------------------------------------------------------------------------------
create table segments(
    segment_id String,
    users Array(String)
) Engine = MergeTree()
Order by (segment_id)

insert into segments values
('s11', ['a1','a2','a3','a4','a5','a6','a7','a8','a9','a10']),
('s10',['a2','a4','a6']),
('s9',['a1','a2','a3']),
('s8',['a1','a2','a3','a4']),
('s7',['a2','a4','a6']),
('s6',['a2']),
('s5',['a2','a3']),
('s4',['a1','a2']),
('s3',['a1','a2']),
('s2',['a2','a3']),
('s1',['a1','a2']),




create table profiles(
    anonymous_id String,
    num_properties Nested(
        keys String,
        vals Float32
    ),
    str_properties Nested(
        keys String,
        vals String
    ),
    created_at DateTime
)Engine = MergeTree()
Order By (anonymous_id, created_at)

insert into profiles values
('a1',['age'],[20], [],[], '2020-07-22 00:00:00'),
('a2', ['age'],[22], ['gender'],['female'],'2020-07-23 00:00:00'),
('a3', ['age'],[18], ['gender'],['male'],'2020-07-24 00:00:00'),

('a4', ['age'],[22], ['gender'],['female'],'2020-07-22 00:00:00')
('a5', ['age'],[18], ['gender'],['male'],'2020-07-24 00:00:00'),
('a6', ['age'],[22], ['gender'],['male'],'2020-07-22 00:00:00'),
('a7', ['age'],[18], ['gender'],['other'],'2020-07-24 00:00:00'),
('a8', ['age'],[22], ['gender'],['male'],'2020-07-22 00:00:00'),
('a9', ['age'],[18], ['gender'],['other'],'2020-07-24 00:00:00'),
('a10', ['age'],[22], ['gender'],['other'],'2020-07-22 00:00:00')
-- Question: Segment s1 co bao nhieu nam, bao nhieu nu ??
-- Question: Age trung binh cua s1 la bao nhieu.

create table segment_agg(
    segment_id String,
    time_stamp DateTime,
    metric_name String,
    metrics_agg Nested(
        keys String,
        vals Float32
    )
) Engine = MergeTree()
Order by(segment_id, metric_name,time_stamp )

-- (s1, now(), 'gender', ['male','female'], [2,4]) -- processing...
-- (s1, now(), 'age', ['avg', 'max', 'min'], [30,12, 23]


create materialized view segment_agg_gender_mv
to segment_agg
as select
    segment_id,
     toStartOfTenMinutes(now()) as time_stamp,
    'gender' as metric_name,
    groupArray(gender) as "metrics_agg.keys",
    groupArray(count) as "metrics_agg.vals"
from (
        select
            segment_id,
            p.gender,
            count() as count
        from segments array join users
        join (
            select
                anonymous_id,
                str_properties.vals[indexOf(str_properties.keys, 'gender')] as gender
            from profiles
        ) as p
        on (p.anonymous_id  = segments.users)
        where gender != '' and gender is not null
        group by segment_id, gender
        order by count desc, gender
    )
group by segment_id, time_stamp


select * from segment_agg
where segment_id = 's1'
    and metric_name = 'gender'
    and time_stamp = '2020-07-22 00:00:00'


--============================================== ReplacingMergeTree================================================
create table profiles(
    anonymous_id String,
    num_properties Nested(
        keys String,
        vals Float32
    ),
    str_properties Nested(
        keys String,
        vals String
    ),
    created_at DateTime
)Engine = MergeTree()
Order By (anonymous_id, created_at)

create table segments(
    segment_id String,
    users Array(String)
) Engine = MergeTree()
Order by (segment_id)

create table segment_agg(
    segment_id String,
    time_stamp DateTime,
    metric_name String,
    metrics_agg Nested(
        keys String,
        vals Float32
    ),
    ack_time DateTime DEFAULT toDateTime(0)
) Engine = ReplacingMergeTree(ack_time)
PARTITION BY toYYYYMM(time_stamp)
Order by(segment_id, time_stamp, metric_name)


create materialized view segment_agg_gender_mv
to segment_agg
as select
    segment_id,
     toStartOfTenMinutes(now()) as time_stamp,
    'gender' as metric_name,
    groupArray(gender) as "metrics_agg.keys",
    groupArray(count) as "metrics_agg.vals",
    now() as ack_time
from (
        select
            segment_id,
            p.gender,
            count() as count
        from segments array join users
        join (
            select
                anonymous_id,
                str_properties.vals[indexOf(str_properties.keys, 'gender')] as gender
            from profiles
        ) as p
        on (p.anonymous_id  = segments.users)
        where gender != '' and gender is not null
        group by segment_id, gender
        order by count desc, gender
    )
group by segment_id, time_stamp



 insert into profiles values
('a1',[],[], ['gender','location_city','context_campaign_source'],['male','HN','facebook'], '2020-07-22 00:00:00'),
('a2',[],[], ['gender','location_city','context_campaign_source'],['male','HCM','google'], '2020-07-22 00:00:00'),
('a3',[],[], ['gender','location_city','context_campaign_source'],['male','HCM','facebook'], '2020-07-22 00:00:00'),
('a4',[],[], ['gender','location_city','context_campaign_source'],['female','HN','google'], '2020-07-12 00:00:00'),
('a5',[],[], ['gender','location_city','context_campaign_source'],['male','HCM','facebook'], '2020-07-06 00:00:00'),
('a6',[],[], ['gender','location_city','context_campaign_source'],['other','HP','youtube'], '2020-07-16 00:00:00'),
('a7',[],[], ['gender','location_city','context_campaign_source'],['male','HCM','facebook'], '2020-07-04 00:00:00'),
('a8',[],[], ['gender','location_city','context_campaign_source'],['female','DN','facebook'], '2020-07-06 00:00:00'),
('a9',[],[], ['gender','location_city','context_campaign_source'],['other','HP','youtube'], '2020-07-16 00:00:00'),
('a10',[],[],['gender','location_city','context_campaign_source'],['female','VT','google'], '2020-07-04 00:00:00')












--=====================================================Location==============================

create materialized view segment_agg_location_mv
to segment_agg
as select
    segment_id,
     toStartOfTenMinutes(now()) as time_stamp,
    'location_city' as metric_name,
    groupArray(location_city) as "metrics_agg.keys",
    groupArray(count) as "metrics_agg.vals",
    now() as ack_time
from (
    select
        segment_id,
        location_city,
        count() as count
    from segments
    array join users
    join (
        select anonymous_id,
            str_properties.vals[indexOf(str_properties.keys, 'location_city')] as location_city
        from profiles ) as p
    on segments.users = p.anonymous_id
    where location_city != '' and location_city is not null
    group by segment_id, location_city
    order by count DESC, location_city
)
group by segment_id, time_stamp





select * from segment_agg final where segment_id = 's1' and metric_name = 'location_city'


--=============================================context_campaign_source ===================================


create materialized view segment_agg_source_mv
to segment_agg
as select
    segment_id,
     toStartOfTenMinutes(now()) as time_stamp,
    'context_campaign_source' as metric_name,
    groupArray(source) as "metrics_agg.keys",
    groupArray(count) as "metrics_agg.vals",
    now() as ack_time
from (
    select
        segment_id,
        source,
        count() as count
    from segments
    array join users
    join (
        select
            anonymous_id,
            str_properties.vals[indexOf(str_properties.keys, 'context_campaign_source')] as source
        from profiles ) as p
    on segments.users = p.anonymous_id
    where source != '' and source is not null
    group by segment_id, source
    order by count DESC, source
)
group by segment_id, time_stamp




--=========================================TEST========================================================
insert into segments values
('s3',['a1','a2','a5']),
('s2',['a2','a3','a6','a7']),
('s1',['a1','a2','a4'])

insert into segments
select
    concat('s', toString(number)) as segment_id,
    arrayFilter((x,y) -> y, ['a1','a2','a3','a4','a5','a6','a7','a8','a9','a10'] as arr, arrayMap(x -> rand(x)%2 , arrayEnumerate(arr))) as users
from system.numbers
limit 10


select * from segment_agg final where segment_id = 's2' and metric_name = 'location_city'

select * from segment_agg where segment_id = 's2'

select * from segment_agg
where segment_id = 's2' and time_stamp = toStart


--================================================LAST_POIN===================================================
create table profiles(
    anonymous_id String,
    num_properties Nested(
        keys String,
        vals Float32
    ),
    str_properties Nested(
        keys String,
        vals String
    ),
    created_at DateTime
)Engine = MergeTree()
Order By (anonymous_id, created_at)

create table segments(
    segment_id String,
    users Array(String)
) Engine = MergeTree()
Order by (segment_id)

create table segment_agg(
    segment_id String,
    time_stamp DateTime,
    metric_name String,
    metrics_agg Nested(
        keys String,
        vals Float32
    )
) Engine = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
Order by(segment_id, time_stamp, metric_name)

create materialized view segment_agg_gender_mv
to segment_agg
as
select
    segment_id,
     toStartOfTenMinutes(now()) as time_stamp,
    'gender' as metric_name,
    groupArray(gender) as "metrics_agg.keys",
    groupArray(count) as "metrics_agg.vals"
from (
        select
            segment_id,
            p.gender,
            count() as count
        from segments array join users
        join (
            select
                anonymous_id,
                str_properties.vals[indexOf(str_properties.keys, 'gender')] as gender
            from profiles
        ) as p
        on (p.anonymous_id  = segments.users)
        where gender != '' and gender is not null
        group by segment_id, gender
        order by count desc, gender
)
group by segment_id, time_stamp


create materialized view segment_agg_location_mv
to segment_agg
as select
    segment_id,
     toStartOfTenMinutes(now()) as time_stamp,
    'location_city' as metric_name,
    groupArray(location_city) as "metrics_agg.keys",
    groupArray(count) as "metrics_agg.vals"
from (
    select
        segment_id,
        location_city,
        count() as count
    from segments
    array join users
    join (
        select anonymous_id,
            str_properties.vals[indexOf(str_properties.keys, 'location_city')] as location_city
        from profiles ) as p
    on segments.users = p.anonymous_id
    where location_city != '' and location_city is not null
    group by segment_id, location_city
    order by count DESC, location_city
)
group by segment_id, time_stamp


create materialized view segment_agg_source_mv
to segment_agg
as select
    segment_id,
     toStartOfTenMinutes(now()) as time_stamp,
    'context_campaign_source' as metric_name,
    groupArray(source) as "metrics_agg.keys",
    groupArray(count) as "metrics_agg.vals"
from (
    select
        segment_id,
        source,
        count() as count
    from segments
    array join users
    join (
        select
            anonymous_id,
            str_properties.vals[indexOf(str_properties.keys, 'context_campaign_source')] as source
        from profiles ) as p
    on segments.users = p.anonymous_id
    where source != '' and source is not null
    group by segment_id, source
    order by count DESC, source
)
group by segment_id, time_stamp



create table segment_last_point_agg(
    segment_id String,
    time_stamp DateTime,
    metric_name String,
    metrics_agg Nested(
        keys String,
        vals Float32
    ),
    ack_time DateTime
) Engine = ReplacingMergeTree(ack_time)
Order by(segment_id, metric_name)


create materialized view segment_last_point_mv
to segment_last_point_agg
as select
    segment_id,
    time_stamp,
    metric_name,
    metrics_agg.keys,
    metrics_agg.vals,
    now() as ack_time
from segment_agg

------------------------------------------- TEST
insert into segments values
('s3',['a1','a2','a5']),
('s2',['a2','a3','a6','a7']),
('s1',['a1','a2'])

 select * from segment_last_point_agg final where segment_id = 's19556' and metric_name = 'gender'

 select count(*) from segment_last_point_agg final

insert into segments
select
    concat('s', toString(number)) as segment_id,
    arrayFilter((x,y) -> y, ['a1','a2','a3','a4','a5','a6','a7','a8','a9','a10'] as arr, arrayMap(x -> rand(x)%2 , arrayEnumerate(arr))) as users
from system.numbers
limit 1000000

-- Test 5 segment:
-- Tested 1000 segment: OK
-- Tested 100000 segment OK
-- Tested 1000000 segment -> Bug 2

--=============================================== BUG========================================================




-- BUG 1: Neu insert 2 segment cung id nhu sau ?
insert into segments values
('s1',['a1','a2']),
('s1',['a1','a2','a3'])
--
┌─segment_id─┬─gender─┬─count─┐
│ s1         │ male   │     5 │
└────────────┴────────┴───────┘

-- BUG 2: Tai sao time_stamp khac nhau, anh huong nhu the nao ?
┌─segment_id─┬──────────time_stamp─┬─metric_name───┬─metrics_agg.keys──────────┬─metrics_agg.vals─┬────────────ack_time─┐
│ s7124      │ 2020-07-24 02:40:00 │ gender        │ ['other','female','male'] │ [2,1,1]          │ 2020-07-24 02:50:05 │
│ s7124      │ 2020-07-24 02:40:00 │ location_city │ ['HP','HN','VT']          │ [2,1,1]          │ 2020-07-24 02:50:07 │
└────────────┴─────────────────────┴───────────────┴───────────────────────────┴──────────────────┴─────────────────────┘
┌─segment_id─┬──────────time_stamp─┬─metric_name─────────────┬─metrics_agg.keys────────────────┬─metrics_agg.vals─┬────────────ack_time─┐
│ s7124      │ 2020-07-24 02:50:00 │ context_campaign_source │ ['youtube','facebook','google'] │ [2,1,1]          │ 2020-07-24 02:50:08 │
└────────────┴─────────────────────┴─────────────────────────┴─────────────────────────────────┴──────────────────┴─────────────────────┘




-------------------------------------------------TODO----------------------------------------------


-- TODO: (s1, now(), 'age', ['avg', 'max', 'min'], [30,12, 23] -- doing..
-- TODO: How to refactor query, -> de sau


- Chia ra str_props | num_prop
- str_prop : count
- num_prop : min, max, avg, sum





---------------------------------------------SCORE_COMMON-------------------------------------------


 insert into profiles values
('a1',['score'],[1], ['gender','location_city','context_campaign_source'],['male','HN','facebook'], '2020-07-22 00:00:00'),
('a2',['score'],[2], ['gender','location_city','context_campaign_source'],['male','HCM','google'], '2020-07-22 00:00:00'),
('a3',['score'],[3], ['gender','location_city','context_campaign_source'],['male','HCM','facebook'], '2020-07-22 00:00:00'),
('a4',['score'],[4], ['gender','location_city','context_campaign_source'],['female','HN','google'], '2020-07-12 00:00:00'),
('a5',['score'],[5], ['gender','location_city','context_campaign_source'],['male','HCM','facebook'], '2020-07-06 00:00:00'),
('a6',['score'],[6], ['gender','location_city','context_campaign_source'],['other','HP','youtube'], '2020-07-16 00:00:00'),
('a7',['score'],[7], ['gender','location_city','context_campaign_source'],['male','HCM','facebook'], '2020-07-04 00:00:00'),
('a8',['score'],[8], ['gender','location_city','context_campaign_source'],['female','DN','facebook'], '2020-07-06 00:00:00'),
('a9',['score'],[9], ['gender','location_city','context_campaign_source'],['other','HP','youtube'], '2020-07-16 00:00:00'),
('a10',['score'],[10],['gender','location_city','context_campaign_source'],['female','VT','google'], '2020-07-04 00:00:00')


create table segment_agg(
    segment_id String,
    time_stamp DateTime,
    metric_name String,
    metrics_agg Nested(
        keys String,
        vals Float32
    )
) Engine = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
Order by(segment_id, time_stamp, metric_name)


create materialized view segment_agg_score_mv
to segment_agg
as
select
    segment_id,
     toStartOfTenMinutes(now()) as time_stamp,
    'score' as metric_name,
    groupArray(score) as "metrics_agg.keys",
    groupArray(count) as "metrics_agg.vals"
from (
        select
            segment_id,
            p.score,
            count() as count
        from segments array join users
        join (
                select
                    anonymous_id,
                    num_properties.vals[indexOf(num_properties.keys, 'score')] as score
                from profiles
        ) as p
        on (p.anonymous_id  = segments.users)
        where score is not null
        group by segment_id, score
        order by count desc, score
)
group by segment_id, time_stamp




--================================================REVENUE===================================


create table events(
    id String,
    tenant_id UInt16,
    at DateTime,
    name String,
    user_id String,
    session_id String,
    scope String,
    num_properties Nested(
        keys LowCardinality(String),
        vals Float32
    ),
    str_properties Nested(
        keys LowCardinality(String),
        vals String
    ),
    arr_properties Nested(
        keys LowCardinality(String),
        vals Array(String)
    )
) Engine = MergeTree()
Partition by toYYYYMMDD(at)
Order by (tenant_id, name, toStartOfHour(at))
SETTINGS index_granularity = 8192

insert into events values
('e1', 1, '2020-01-01 00:00:00', 'add_to_cart', 'u1', 'sess1','ecommerce',
 ['target.properties.total_value','target.properties.discount_value'],[100,10.5],
 ['target.properties.category','target.properties.currency'],['iphone7','USD'],
 [],[] )

insert into events values
('e2', 1, '2020-01-01 00:01:00', 'add_to_cart', 'u2', 'sess2','ecommerce',
 ['target.properties.total_value','target.properties.discount_value'],[80,0],
 ['target.properties.category','target.properties.currency'],['iphone7','USD'],
 [],[] )

insert into events values
('e3', 1, '2020-01-01 00:03:00', 'add_to_cart', 'u1', 'sess1','ecommerce',
 ['target.properties.total_value','target.properties.discount_value'],[160,30],
 ['target.properties.category','target.properties.currency'],['iphone8','USD'],
 [],[] )

insert into events values
('e4', 1, '2020-01-01 00:04:00', 'add_to_cart', 'u3', 'sess3','ecommerce',
 ['target.properties.total_value','target.properties.discount_value'],[220,30],
 ['target.properties.category','target.properties.currency'],['iphoneX','USD'],
 [],[] )
-- event with user u4 not in segment s1
 insert into events values
('e5', 1, '2020-01-01 00:04:00', 'add_to_cart', 'u4', 'sess4','ecommerce',
 ['target.properties.total_value','target.properties.discount_value'],[220,30],
 ['target.properties.category','target.properties.currency'],['iphoneX','USD'],
 [],[] )

 -- a event is not conversion event
  insert into events values
 ('e6', 1, '2020-01-01 00:01:00', 'view', 'u1', 'sess1','ecommerce',
 [],[],
 ['target.properties.category'],['iphoneX'],
 [],[] )


CREATE TABLE segments(
    segment_id String,
    state String,
    user_id String,
    at DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (segment_id, at)
SETTINGS index_granularity = 8192
-- state: OptIn

insert into segments values
('s1', 'OptIn', 'u1', '2020-01-01 00:00:00'),
('s1', 'OptIn', 'u2', '2020-01-01 00:01:00'),
('s1', 'OptIn', 'u3', '2020-01-01 00:02:00'),
('s2', 'OptIn', 'u1', '2020-01-01 00:03:00')

create table segment_agg(
    segment_id String,
    time_stamp DateTime,
    metric_name String,
    metrics_agg Nested(
        keys String,
        vals Float32
    )
) Engine = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
Order by(segment_id, time_stamp, metric_name)

-- Track total value
CREATE TABLE user_total_value_daily (
    day DateTime,
    user_id String,
    property String,
    count_value_state AggregateFunction(count, Float32),
    sum_value_state AggregateFunction(sum, Float32)
)
ENGINE = SummingMergeTree()
PARTITION BY tuple()
ORDER BY (user_id, day)


create materialized view user_total_value_daily_mv
to user_total_value_daily
as
select
    toStartOfDay(at) as day,
    user_id,
    'total_value' as property,
    countState(num_properties.vals[indexOf(num_properties.keys,'target.properties.total_value')]) as count_value_state,
    sumState(num_properties.vals[indexOf(num_properties.keys,'target.properties.total_value')]) as sum_value_state
from events
group by user_id, day
order by user_id, day


select
    day,
    user_id,
    property,
    countMerge(count_value_state),
    sumMerge(sum_value_state),
from user_total_value_daily






select
    user_id,
    num_properties.vals[indexOf(num_properties.keys,'target.properties.total_value')] as total_value
from events
group by user_id




























--------------------------------------------------TEMP
    max_value_state AggregateFunction(max, Float32),
    min_value_state AggregateFunction(min, Float32),
    avg_value_state AggregateFunction(avg, Float32)




----------------------------------------------EDIT PROFILE
CREATE TABLE user_profile(

    tenant UInt16,
    user_id String,
    anonymous_id String,
    identifies Array(String),

    str_properties Nested(
        keys String,
        vals String
    ),
    num_properties Nested(
        keys String,
        vals Float32
    ),
    arr_properties Nested(
        keys String,
        vals Array(String)
    ),
    create_at DateTime

) Engine MergeTree()
Primary key (tenant, user_id, create_at)
Order By (tenant, user_id, create_at)

insert into user_profile values
(1,'u1','a1',[] ,[],[],[],[],[],[], '2020-01-01 00:00:00')
insert into user_profile values
(1,'u1','a1',[] ,[],[],[],[],[],[], '2020-01-02 00:00:00')

insert into user_profile values
(1,'u1','a1',[] ,[],[],[],[],[],[], '2020-01-03 00:00:00')
insert into user_profile values
(1,'u1','a1',[],[],[],[],[],[],[], '2020-01-03 00:01:00')
insert into user_profile values
(1,'u1','a1',[],[],[],[],[],[],[], '2020-01-03 00:01:01')


SELECT OUTSIDE* FROM user_profile OUTSIDE,
(SELECT tenant_id, anonymous_id, max(created_at) AS max_created_at
FROM user_profile
WHERE tenant_id = 1 AND anonymous_id = 'a1' group BY tenant_id, anonymous_id) AS INSIDE
WHERE OUTSIDE.created_at = INSIDE.max_created_at
AND OUTSIDE.tenant_id = INSIDE.tenant_id
AND OUTSIDE.anonymous_id = INSIDE.anonymous_id

