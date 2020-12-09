__tablename__ = 'conversion_paths'
    tenant_id = Column(types.UInt16, primary_key=True)
    universal_id = Column(types.UInt64)
    path_name = Column(types.String)

    path = Column(types.Array(types.String))
    time_stamps = Column(types.Array(types.UInt64))
    conversion_value = Column(types.Float32)

    at = Column(types.DateTime)

show tables;
drop table conversion_paths;
CREATE TABLE conversion_paths
(
    tenant_id             UInt16,
    universal_id          String,
    path_name             String,
    point Nested(
        path String,
        time_stamp UInt64
    ),
    number_of_conversions Float32,
    conversion_value      Float32,
    execution_time        DateTime,
    window                UInt16,
    created_at            DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (tenant_id, universal_id, path_name, created_at)
;

select *
from conversion_paths
order by point.path;

truncate table conversion_paths;
insert into conversion_paths
values (1, 'u1', 'touch_type', ['direct', 'organic', 'paid'], [1,3,5], 1, 10,now(),14, now()),
       (1, 'u1', 'touch_type', ['direct', 'paid'], [1,2], 1, 10,now(),14, now()),
       (1, 'u1', 'touch_type', ['organic', 'paid'], [3,5], 1, 10,now(),14, now())
;
insert into conversion_paths
values (1, 'u1', 'touch_type', ['direct', 'paid'], [2,5], 3, 8, now(),14, now()),;
insert into conversion_paths
values (1, 'u1', 'touch_type', ['paid'], [5], 3, 8,now(), 14, now()),;


select tenant_id,
       path_name,
       `point.path`,
       sumArray(`point.time_stamp`),
       sum(number_of_conversions),
       sum(conversion_value)
from conversion_paths
group by tenant_id, path_name, point.path;


CREATE VIEW conversion_paths_analytic_v AS
SELECT tenant_id,
       path_name,
       paths,
       avgForEach(time_diffs)     AS avg_time_lag,
       sum(number_of_conversions) AS number_of_conversion,
       sum(conversion_value)      AS total_value
FROM (
      SELECT tenant_id,
             path_name,
             `point.path`                                     AS paths,
             arrayPopFront(arrayDifference(point.time_stamp)) AS time_diffs,
             number_of_conversions,
             conversion_value
      FROM conversion_paths
         )
GROUP BY tenant_id, path_name, paths;


SELECT tenant_id,
       path_name,
       paths,
       avg_time_lag,
       number_of_conversion,
       total_value
FROM conversion_paths_analytic_v
WHERE tenant_id = 1;


SELECT tenant_id,
       path_name,
       paths,
       avg_time_lag,
       number_of_conversion,
       total_value
FROM conversion_paths_analytic_v
WHERE tenant_id = 1;


show create table conversion_paths_analytic_v;

drop table conversion_paths_analytic_v;
CREATE VIEW eventify_stag.conversion_paths_analytic_v
AS
SELECT
    tenant_id,
    path_name,
    paths,
    execution_time,
    window,
    avgForEach(time_diffs) AS avg_time_lag,
    sum(number_of_conversions) AS number_of_conversion,
    sum(conversion_value) AS total_value
FROM
(
    SELECT
        tenant_id,
        path_name,
        `point.path` AS paths,
        arrayPopFront(arrayDifference(point.time_stamp)) AS time_diffs,
        number_of_conversions,
        conversion_value,
        execution_time,
        window
    FROM eventify_stag.conversion_paths
)
GROUP BY
    tenant_id,
    path_name,
    paths,
    execution_time,
    window
;





SELECT tenant_id,
       path_name,
       paths,
       avg_time_lag,
       number_of_conversion,
       total_value,
       toUnixTimestamp(execution_time) as execution_time,
       window
FROM conversion_paths_analytic_v
WHERE tenant_id = 1 and path_name = 'touch_type' and execution_time <= toUnixTimestamp(now())
GROUP BY tenant_id, path_name, paths, avg_time_lag, number_of_conversion, total_value
;


SELECT argMax(tenant_id, execution_time),
       argMax(path_name, execution_time),
       argMax(paths, execution_time),
       argMax(avg_time_lag, execution_time),
       argMax(number_of_conversion, execution_time),
       argMax(total_value, execution_time),
       max(toUnixTimestamp(execution_time)) as execution_time,
       argMax(window, execution_time)
FROM conversion_paths_analytic_v
WHERE tenant_id = 1 and path_name = 'touch_type' and execution_time <= toUnixTimestamp(now())
GROUP BY tenant_id, path_name, paths, avg_time_lag, number_of_conversion, total_value
;

SELECT tenant_id,
       path_name,
       paths,
       argMax(avg_time_lag, execution_time) as avg_time_lag,
       argMax(number_of_conversion, execution_time) as ,
       argMax(total_value, execution_time),
       max(execution_time) as execution_time_lastest,
       argMax(window, execution_time)
FROM (
    SELECT tenant_id,
           path_name,
           paths,
           avg_time_lag,
           number_of_conversion,
           total_value,
           execution_time,
           window
    FROM conversion_paths_analytic_v
    WHERE tenant_id = 1
      and path_name = 'touch_type'
      and execution_time <= now()
      and execution_time >= subtractDays(now(),14)
      and window = 14
)
GROUP BY tenant_id, path_name, paths
;

select * from conversion_paths;
truncate table conversion_paths;

show create table conversion_paths;


select toUnixTimestamp(now());
