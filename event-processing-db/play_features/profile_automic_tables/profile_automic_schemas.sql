show tables;

--------------------------------Create new table
show create table profile_str;
CREATE TABLE eventify_stag.profile_str
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `str_key` LowCardinality(String),
    `str_val` String,
    `at` DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, str_key, at);


show create table profile_num;
CREATE TABLE eventify_stag.profile_num
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `num_key` LowCardinality(String),
    `num_val` Float64,
    `at` DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, num_key, at);

drop table profile_arr;
CREATE TABLE eventify_stag.profile_arr
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `arr_key` LowCardinality(String),
    `arr_val` Array(String),
    `at` DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, arr_key, at);


show create table user_profile;
CREATE TABLE eventify_stag.user_profile
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
ORDER BY (tenant_id, anonymous_id, at);



-----------------------------------------GET FINAL
select count() from profile_str; -- 1814849
select count() from profile_num; --347146
select * from profile_num where anonymous_id = 'DDHZCnSEJu7QV9AnM39ChGXwbZ1' and num_key = 'revenue';


select count(*) from user_profile_final_v; --263 ms
select count() from (
select tenant_id,
       anonymous_id,
       argMax(num_key, at),
       argMax(num_val, at),
       max(at)
from profile_num
group by tenant_id, anonymous_id )-- execution: 39066 - 1 s 34 ms

----------------------------------------Create aggregate table final
show create table user_profile_final;
CREATE TABLE eventify_stag.user_profile_final
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `identity.keys` AggregateFunction(argMax, Array(String), DateTime64(3)),
    `identity.vals` AggregateFunction(argMax, Array(String), DateTime64(3)),
    `str_properties.keys` AggregateFunction(argMax, Array(String), DateTime64(3)),
    `str_properties.vals` AggregateFunction(argMax, Array(String), DateTime64(3)),
    `num_properties.keys` AggregateFunction(argMax, Array(String), DateTime64(3)),
    `num_properties.vals` AggregateFunction(argMax, Array(Float64), DateTime64(3)),
    `arr_properties.keys` AggregateFunction(argMax, Array(String), DateTime64(3)),
    `arr_properties.vals` AggregateFunction(argMax, Array(Array(String)), DateTime64(3)),
    `at_final` SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id);

show create user_profile_final_mv;
CREATE MATERIALIZED VIEW eventify_stag.user_profile_final_mv TO eventify_stag.user_profile_final
SELECT
    anonymous_id,
    tenant_id,
    argMaxState(identity.keys, at) AS `identity.keys`,
    argMaxState(identity.vals, at) AS `identity.vals`,
    argMaxState(str_properties.keys, at) AS `str_properties.keys`,
    argMaxState(str_properties.vals, at) AS `str_properties.vals`,
    argMaxState(num_properties.keys, at) AS `num_properties.keys`,
    argMaxState(num_properties.vals, at) AS `num_properties.vals`,
    argMaxState(arr_properties.keys, at) AS `arr_properties.keys`,
    argMaxState(arr_properties.vals, at) AS `arr_properties.vals`,
    max(at) AS at_final
FROM eventify_stag.user_profile
GROUP BY
    tenant_id,
    anonymous_id;


drop table profile_str_final;
CREATE TABLE eventify_stag.profile_str_final
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `str_key` LowCardinality(String),
    `str_val` AggregateFunction(argMax, String, DateTime64(3)),
    `at` SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id, str_key);

drop table profile_num_final;
CREATE TABLE eventify_stag.profile_num_final
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `num_key` LowCardinality(String),
    `num_val` AggregateFunction(argMax, Float64, DateTime64(3)),
    `at` SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id, num_key);

drop table profile_arr_final;
CREATE TABLE eventify_stag.profile_arr_final
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `arr_key` LowCardinality(String),
    `arr_val` AggregateFunction(argMax, Array(String), DateTime64(3)),
    `at` SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id, arr_key);

----------mv final
CREATE MATERIALIZED VIEW eventify_stag.profile_str_final_mv TO eventify_stag.profile_str_final AS
SELECT
    anonymous_id,
    tenant_id,
    str_key AS `str_key`,
    argMaxState(str_val, ps.at) AS `str_val`,
    max(ps.at) AS `at`
FROM eventify_stag.profile_str as ps
GROUP BY
    tenant_id,
    anonymous_id,
    str_key
ORDER BY tenant_id, anonymous_id, str_key;


CREATE MATERIALIZED VIEW eventify_stag.profile_num_final_mv TO eventify_stag.profile_num_final AS
SELECT
    anonymous_id,
    tenant_id,
    num_key AS `num_key`,
    argMaxState(num_val, ps.at) AS `num_val`,
    max(ps.at) AS `at`
FROM eventify_stag.profile_num as ps
GROUP BY
    tenant_id,
    anonymous_id,
    num_key
ORDER BY tenant_id, anonymous_id, num_key;


CREATE MATERIALIZED VIEW eventify_stag.profile_arr_final_mv TO eventify_stag.profile_arr_final AS
SELECT
    anonymous_id,
    tenant_id,
    arr_key AS `arr_key`,
    argMaxState(arr_val, ps.at) AS `arr_val`,
    max(ps.at) AS `at`
FROM eventify_stag.profile_arr as ps
GROUP BY
    tenant_id,
    anonymous_id,
    arr_key
ORDER BY tenant_id, anonymous_id, arr_key;


------------------------------insert data to final
truncate table profile_str;
truncate table profile_num;
truncate table profile_arr;

insert into profile_str
 select anonymous_id,
        tenant_id,
        str_key,
        str_val,
        at
 from eventify.user_profile
     array join
      `str_properties.keys` as str_key,
      `str_properties.vals` as str_val;

-- 1814848

select count() from profile_num;


;
insert into profile_num
 select anonymous_id,
        tenant_id,
        num_key,
        num_val,
        at
 from eventify.user_profile
     array join
      `num_properties.keys` as num_key,
      `num_properties.vals` as num_val;

select count() from profile_num;
select * from profile_num;


insert into profile_arr
 select anonymous_id,
        tenant_id,
        arr_key,
        arr_val,
        at
 from eventify.user_profile
     array join
      `arr_properties.keys` as arr_key,
      `arr_properties.vals` as arr_val;

--------------------------create view

-- str
show create table profile_str_final_v;
show create table user_profile_final_v;
CREATE VIEW eventify_stag.profile_str_final_v
AS
SELECT
    anonymous_id,
    tenant_id,
    str_key,
    argMaxMerge(str_val) AS str_val,
    max(at) AS at
FROM eventify_stag.profile_str_final
GROUP BY
    tenant_id,
    anonymous_id,
    str_key
ORDER BY tenant_id,anonymous_id, str_key;

-- num
CREATE VIEW eventify_stag.profile_num_final_v
AS
SELECT
    anonymous_id,
    tenant_id,
    num_key,
    argMaxMerge(num_val) AS num_val,
    max(at) AS at
FROM eventify_stag.profile_num_final
GROUP BY
    tenant_id,
    anonymous_id,
    num_key
ORDER BY tenant_id,anonymous_id, num_key;

--arr

select * from profile_str_final_v;
select * from user_profile_final_v where tenant_id =1 and anonymous_id = 'DDwd9CjgRqcnZ8jyKEBEGuzfwu1';
select count() from profile_str_final_v;
select count() from eventify.user_profile_final_v;


show tables;



select anonymous_id, tenant_id, num_key, argMaxMerge(num_val) as num_val, max(at)
from profile_num_final
where tenant_id = 1 and anonymous_id = 'DDlBzGnR8JmbQKBBV2TT0TY9CVo'
group by tenant_id, anonymous_id, num_key;

--------------------------test final
select * from user_profile_final where anonymous_id = 'DDHZCnSEJu7QV9AnM39ChGXwbZ1'; -- 198 ms
select * from profile_str_final where anonymous_id = 'DDHZCnSEJu7QV9AnM39ChGXwbZ1' ; -- 328 ms

select count() from profile_str_final_v; 936826
select count() from profile_str; 2251620

select count() from profile_num_final_v; 183876
select count() from profile_num; 488877

select * from profile_str_final_v where anonymous_id = 'DDF32OU8H41jvY70u4Ud9CSwja8';
-------------------------------------------------move sql
select * from profile_str where anonymous_id = 'DDF32OU8H41jvY70u4Ud9CSwja8';
select * from profile_str_final_v where tenant_id = 1 and anonymous_id = 'DDK0cPdAvWo7Zq5ivG2WFHN3EQk' order by at desc ;


-----------------------Get a properties of a profile


-- old table: key value array
SELECT tenant_id,
       anonymous_id,
       'revenue' as num_key,
       num_pros_vals[indexOf(num_pros_keys, 'revenue')] as num_val,
       at
FROM user_profile_final_v
WHERE tenant_id = 1 and anonymous_id = 'DDK0cPdAvWo7Zq5ivG2WFHN3EQk'; -- 151

-- new table: metric per row.
SELECT tenant_id,
       anonymous_id,
       num_key,
       num_val,
       at
FROM profile_num_final_v
WHERE tenant_id = 1
  AND anonymous_id = 'DDK0cPdAvWo7Zq5ivG2WFHN3EQk'
  AND num_key = 'revenue'; --75ms


--------------------------Get some properties of profile
select tenant_id,
       anonymous_id,
       groupArray((num_key, num_val)) as pros,
       max(toUnixTimestamp64Milli(at))                        as at
from profile_num_final_v
where tenant_id = 2
  and anonymous_id = 'DDz2ygovDHBsZL2x1Mr6keJNTiE'
  and num_key IN ['age', 'lat', 'long', 'total_order', 'revenue']
group by tenant_id, anonymous_id;

select * from profile_num_final_v;



------------------------Get all properties of a profile
select tenant_id, anonymous_id,
       groupArray(num_key),
       groupArray(num_val),
       max(at) as at
from profile_num_final_v
where tenant_id = 1 and anonymous_id = 'DDK0cPdAvWo7Zq5ivG2WFHN3EQk'
group by tenant_id, anonymous_id;


select tenant_id,
       anonymous_id,
       str_pros,
       num_pros,
       arrayReduce('max', [ps.at, pn.at]) as at
from
(
    select tenant_id,
           anonymous_id,
           groupArray((str_key, str_val)) as str_pros,
           max(at) as at
    from profile_str_final_v
    where tenant_id = 1
      and anonymous_id = 'DDK0cPdAvWo7Zq5ivG2WFHN3EQk'
      and str_key IN ['city', 'country', 'gender', 'street']
    group by tenant_id, anonymous_id
) as ps
join
(
    select tenant_id,
           anonymous_id,
           groupArray((num_key, num_val)) as num_pros,
           max(at)                        as at
    from profile_num_final_v
    where tenant_id = 1
      and anonymous_id = 'DDK0cPdAvWo7Zq5ivG2WFHN3EQk'
      and num_key IN ['age', 'lat', 'long', 'total_order', 'revenue']
    group by tenant_id, anonymous_id
) as pn
on ps.tenant_id = pn.tenant_id and ps.anonymous_id = pn.anonymous_id;



---------------------Get multi properties of multi user
select tenant_id,
       anonymous_id,
       str_pros,
       num_pros,
       at
from (
    select tenant_id,
           anonymous_id,
           groupArray((str_key, str_val)) as str_pros,
           at
    from profile_str_final_v
    where  str_key IN ['city', 'country', 'gender', 'street']
    group by tenant_id, anonymous_id, at
) as ps
    inner join
(

    select tenant_id,
           anonymous_id,
           groupArray((num_key, num_val)) as num_pros,
           at
    from profile_num_final_v
    where num_key IN ['age', 'lat', 'long', 'total_order', 'revenue']
    group by tenant_id, anonymous_id, at
    ) as pn on (ps.tenant_id = pn.tenant_id
                    and ps.anonymous_id = pn.anonymous_id
                    and ps.at = pn.at)
limit 9;








--GET_LATEST_PROFILE_FILTER_PROS_KEY
SELECT
    tenant_id,
    anonymous_id,
    arrayZip(identity_keys, identity_vals) as identities,
    arrayFilter(x -> ((x.1) IN :str_pros_filter), arrayZip(str_pros_keys, str_pros_vals)) AS str_pros,
    arrayFilter(x -> ((x.1) IN :num_pros_filter), arrayZip(num_pros_keys, num_pros_vals)) AS num_pros,
    arrayFilter(x -> ((x.1) IN :arr_pros_filter), arrayZip(arr_pros_keys, arr_pros_vals)) AS arr_pros,
    at
FROM user_profile_final_v
WHERE tenant_id = :tenant_id AND anonymous_id = :anonymous_id




SELECT
    tenant_id,
    anonymous_id,
    `str_pros_vals`[indexOf(`str_pros_keys`, 'gender')] AS gender,
    `str_pros_vals`[indexOf(`str_pros_keys`, 'city')] AS location_city,
    `str_pros_vals`[indexOf(`str_pros_keys`, 'device_platform')] AS device
FROM user_profile_final_v
WHERE
      tenant_id = 1
      AND anonymous_id IN
        (
            SELECT anonymous_id
            FROM events_campaign
            WHERE utm_campaign = :utm_campaign
        )

--------------
;

show create table profile_str_final_v;


select now64(); --2020-11-16 02:27:58.132
select toUnixTimestamp64Milli('2020-11-16 02:27:58.132');
select * from profile_str; --2020-11-16 02:29:36.417
insert into profile_str values
('a1', 1, 'gender', 'Male', now64());

select toUnixTimestamp64Milli(at) from profile_str; -- 1605493776417
select toUnixTimestamp(at) from profile_str; --        1605493776
select toDateTime(at) from profile_str; -- 2020-11-16 02:29:36
select toStartOfDay(at) from profile_str; --2020-11-16 00:00:00
select toDate(at) from profile_str; --2020-11-16



-------------------------------------------min max avg















