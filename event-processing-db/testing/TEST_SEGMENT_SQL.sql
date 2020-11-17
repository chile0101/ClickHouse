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
            WHERE tenant_id = 1 AND segment_id ='1jP08tvZXG9vJj7uJ4okEwCCLlE'
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
            WHERE tenant_id =1
            GROUP BY
                tenant_id,
                anonymous_id
        )
    ) AS pf ON (sf.tenant_id = pf.tenant_id) AND (sf.users = pf.anonymous_id)
    WHERE cate != ''
    GROUP BY
        tenant_id,
        segment_id,
        cate
    ORDER BY
        count DESC,
        cate ASC
);















SELECT
   *
-- median(v) as median,
-- quantile(v) as quatile
--     ['max', 'avg', 'med', 'min'] AS metric_key,
--     [maxOrDefault(v), avgOrDefault(v), medianOrDefault(v), minOrDefault(v)] AS metric_value
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
        WHERE tenant_id = 1 AND segment_id ='1jP08tvZXG9vJj7uJ4okEwCCLlE'
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
        WHERE tenant_id = 1
        GROUP BY
                 tenant_id,
                 anonymous_id
    )
) AS pf ON (sf.tenant_id = pf.tenant_id) AND (sf.users = pf.anonymous_id)
WHERE v >= 0
;









SELECT time_stamps[idx] AS time_stamp,
       total_users[idx] AS value
FROM
(
    SELECT
        groupArray(time_stamp) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
        SELECT
            toStartOfFifteenMinutes(at) AS time_stamp,
            round(avg(length(users))) AS total_user
        FROM
        (
            SELECT DISTINCT *
            FROM segment_users
            WHERE (tenant_id = 1)
                AND (segment_id ='1jP08tvZXG9vJj7uJ4okEwCCLlE')
                AND (at <= now())
        )
        GROUP BY
            tenant_id,
            segment_id,
            time_stamp
        ORDER BY time_stamp WITH FILL FROM toDateTime('2020-10-26 00:00:00') TO now() STEP 900
    )
)
ARRAY JOIN arrayEnumerate(time_stamps) as idx
WHERE time_stamp >= %(start_time)s
  AND time_stamp <= %(end_time)s




select toUnixTimestamp('2020-10-25 00:00:00') as start,
       toUnixTimestamp(now()) as end;


SELECT
    pf.cate as key,
    count() AS value
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
        WHERE tenant_id = 1 AND segment_id = '1jP08tvZXG9vJj7uJ4okEwCCLlE'
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
) AS pf ON (sf.tenant_id = pf.tenant_id) AND (sf.users = pf.anonymous_id)
WHERE cate != ''
GROUP BY
    tenant_id,
    segment_id,
    cate
ORDER BY
    value DESC,
    key ASC


;

