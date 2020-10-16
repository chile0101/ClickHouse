
show tables;

select histogram(5)(number + 1)
from
(
select * from system.numbers limit 20
    )


WITH histogram(5)(rand() % 100) AS hist
SELECT
    arrayJoin(hist).3 AS height
FROM
(
    SELECT *
    FROM system.numbers
    LIMIT 20
)
;

-------------------------------------------------------------REVENUE-----------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS segment_agg_revenue_mv TO segment_agg AS
SELECT
    sf.tenant_id,
    sf.segment_id,
    now() AS time_stamp,
    'total_revenue' AS metric_name,
    ['max', 'avg', 'med', 'min'] AS `metrics_agg.keys`,
    [max(v), avg(v), median(v), min(v)] AS `metrics_agg.vals`
FROM
(
    SELECT
        tenant_id,
        segment_id,
        users
    FROM
    (
        SELECT
            tenant_id,
            segment_id,
            argMaxMerge(users) AS users
        FROM segment_users_final
        GROUP BY
            tenant_id,
            segment_id
    )
    ARRAY JOIN users
) AS sf
INNER JOIN
(
    SELECT
        tenant_id,
        anonymous_id,
        num_vals[indexOf(num_keys, 'total_revenue')] AS v
    FROM
    (
        SELECT
            tenant_id,
            anonymous_id,
            argMaxMerge(num_properties.keys) AS num_keys,
            argMaxMerge(num_properties.vals) AS num_vals
        FROM user_profile_final
        GROUP BY
                 tenant_id,
                 anonymous_id
    )
) AS pf ON (sf.users = pf.anonymous_id) AND (sf.tenant_id = pf.tenant_id)
WHERE v > 0
GROUP BY
    tenant_id,
    segment_id
;


select
    groupArray(total_revenue)
from (
         select tenant_id,
                segment_id,
                users
         from segment_users_final_v array join users
        where tenant_id = 1 and segment_id = 's1'
)  as s
    inner join
     (
         select tenant_id,
                anonymous_id,
                num_pros_vals[indexOf(num_pros_keys, 'total_revenue')] as total_revenue
         from user_profile_final_v
         where tenant_id = 1
    ) as p
on (s.tenant_id = p.tenant_id) and (s.users = p.anonymous_id)
;



"histogram": {
      "numberOfBins": 5,
      "data": [
        {
        "index" : 0,
        "lower_bound": 30,
        "upper_bound": 46,
        "heigh": 3,
        },
        {

        }

      ]
    }





select histogram(5)(number)
from (
     select number from system.numbers limit 10
)
;




SELECT
    groupArray(total)
FROM
(
    SELECT
        tenant_id,
        segment_id,
        users
    FROM segment_users_final_v
    ARRAY JOIN users
    WHERE (tenant_id = 1) AND (segment_id = 's1')
) AS s
INNER JOIN
(
    SELECT
        tenant_id,
        anonymous_id,
        num_pros_vals[indexOf(num_pros_keys, 'avg_order_value')] AS total
    FROM user_profile_final_v
    WHERE tenant_id = 1
) AS p ON (s.tenant_id = p.tenant_id) AND (s.users = p.anonymous_id);

select * from user_profile_final_v;