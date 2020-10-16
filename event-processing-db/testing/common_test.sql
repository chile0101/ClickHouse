SELECT 
    t1.data_point AS data_point,
    t2.value AS value
FROM 
(
    SELECT data_point
    FROM 
    (
        SELECT toStartOfDay(now()) + ((number * 60) * 60) AS data_point
        FROM system.numbers
        LIMIT 24
    )
    WHERE data_point <= now()
) AS t1
LEFT JOIN 
(
    SELECT 
        time_stamp,
        uniqMerge(value) AS value
    FROM event_total_user_by_source_ts
    WHERE (tenant_id = 1)
        AND (source_id = 'IOS-1gazgMAxIp60M6k9YE3HdghgRda')
        AND (time_stamp >= toStartOfDay(now())) 
        AND (time_stamp < now())
    GROUP BY 
        tenant_id,
        source_id,
        time_stamp
    ORDER BY 
        tenant_id ASC,
        source_id ASC,
        time_stamp ASC
) AS t2 ON t1.data_point = t2.time_stamp
;

-- select now();
-- select toDateTime(now(), 'Asia/Hong_Kong')
-- select toDateTime(now(), 'Asia/Ho_Chi_Minh')


CREATE MATERIALIZED VIEW IF NOT EXISTS segment_agg_days_since_last_order_mv TO segment_agg AS
SELECT
    tenant_id,
    segment_id,
    now() AS time_stamp,
    'days_since_last_order' AS metric_name,
    ['max', 'avg', 'med', 'min'] AS `metrics_agg.keys`,
    [toFloat64(max(days)), avg(days), median(days), toFloat64(min(days))] AS `metrics_agg.vals`
FROM
(
    SELECT
        sf.tenant_id,
        sf.segment_id,
        round((toUnixTimestamp(now()) - t) / ((24 * 60) * 60)) AS days
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
            num_vals[indexOf(num_keys, 'days_since_last_order')] AS t
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
    WHERE t >= 0
)
GROUP BY
    tenant_id,
    segment_id
;





SELECT
    tenant_id,
    segment_id,
    now() AS time_stamp,
    'days_since_last_order' AS metric_name,
    ['max', 'avg', 'med', 'min'] AS `metrics_agg.keys`,
    [toFloat64(max(days)), avg(days), median(days), toFloat64(min(days))] AS `metrics_agg.vals`
FROM
(
    SELECT
        sf.tenant_id,
        sf.segment_id,
        round((toUnixTimestamp(now()) - t) / ((24 * 60) * 60)) AS days
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
            num_pros_vals[indexOf(num_pros_keys, 'days_since_last_order')] AS t
        FROM user_profile_final_v
        WHERE tenant_id = 1
    ) AS pf
    ON (sf.tenant_id = pf.tenant_id) AND (sf.users = pf.anonymous_id)
)
GROUP BY
    tenant_id,
    segment_id
;






--------------------------------------GENDER------------------------------
SELECT
    groupArray(cate) AS metric_key,
    groupArray(count) AS metric_value
FROM
(
    SELECT
        pf.cate,
        count() AS count
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
            WHERE segment_id = 's1'
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
            str_vals[indexOf(str_keys, 'gender')] AS cate
        FROM
        (
            SELECT
                tenant_id,
                anonymous_id,
                argMaxMerge(str_properties.keys) AS str_keys,
                argMaxMerge(str_properties.vals) AS str_vals
            FROM user_profile_final
            WHERE tenant_id = 1
            GROUP BY
                tenant_id,
                anonymous_id
        )
    ) AS pf ON (sf.users = pf.anonymous_id) AND (sf.tenant_id = pf.tenant_id)
    WHERE cate != ''
    GROUP BY
        tenant_id,
        segment_id,
        cate
    ORDER BY
        count DESC,
        cate ASC
)
;

----------------------------REVENUE------------------------------

SELECT

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
        WHERE segment_id = 's1'
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
        WHERE tenant_id = 1
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


 SELECT
        ['total_user'] as metric_key,
        [length(users)] as metric_value
    FROM (
          SELECT
                 argMaxMerge(users) as users
          FROM segment_users_final
          WHERE (tenant_id = 1)
            AND (segment_id = 's1')
          GROUP BY tenant_id,
                   segment_id
             )

;



SELECT
 ['max', 'avg', 'med', 'min'] AS metric_key,
 [maxOrDefault(v), avgOrDefault(v), medianOrDefault(v), minOrDefault(v)] AS metric_value
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
        WHERE tenant_id = 1 AND segment_id = 's1'
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
        num_vals[indexOf(num_keys, 'total_order')] AS v
    FROM
    (
        SELECT
            tenant_id,
            anonymous_id,
            argMaxMerge(num_properties.keys) AS num_keys,
            argMaxMerge(num_properties.vals) AS num_vals
        FROM user_profile_final
        WHERE tenant_id = 1
        GROUP BY
                 tenant_id,
                 anonymous_id
    )
) AS pf ON (sf.users = pf.anonymous_id) AND (sf.tenant_id = pf.tenant_id)
WHERE v > 0
;

insert into segment_users values
(25, 's1', ['a1','a2','a3','a4','a5'], now())
;
select * from user_profile_final_v where tenant_id = 25;




SELECT
    ['max', 'avg', 'med', 'min'] AS metric_key,
    [maxOrDefault(v), avgOrDefault(v), medianOrDefault(v), minOrDefault(v)] AS metric_value
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
        WHERE tenant_id = 25 AND segment_id = 's1'
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
        num_vals[indexOf(num_keys, 'revenue')] AS v
    FROM
    (
        SELECT
            tenant_id,
            anonymous_id,
            argMaxMerge(num_properties.keys) AS num_keys,
            argMaxMerge(num_properties.vals) AS num_vals
        FROM user_profile_final
        WHERE tenant_id = 25
        GROUP BY
                 tenant_id,
                 anonymous_id
    )
) AS pf ON (sf.users = pf.anonymous_id) AND (sf.tenant_id = pf.tenant_id)
WHERE v > 0
;


show tables;