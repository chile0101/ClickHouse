show tables;


------------------------------------Data source total user => Can not refactor.
SELECT uniqMerge(value) AS total_user
FROM event_total_user_by_source_ts
WHERE (tenant_id = 1) AND (source_id = 'ADR-1htXpdVcfiprEmhWgs3GSEd13qF')
GROUP BY
    tenant_id,
    source_id;

select * from event_total_user_by_source_ts;
------------------------------------Data source total use time series, fifteen minute => Can not refactor

SELECT
       toUnixTimestamp(t1.time_stamp) AS time_stamp,
       t1.time_stamp as date_time,
       t2.value as value
FROM
(
    WITH
        15 AS delta,
        1600794000 AS start_unix,
        1600834509 AS end_unix,
        FROM_UNIXTIME(start_unix) AS start,
        FROM_UNIXTIME(end_unix) AS end,
        ceil((end_unix - start_unix)/(60*delta)) AS n
    SELECT addMinutes(toStartOfFifteenMinutes(start), number * delta) AS time_stamp
    FROM system.numbers
    LIMIT n
) AS t1
LEFT JOIN
(
    WITH
        FROM_UNIXTIME(1600794000) AS start,
        FROM_UNIXTIME(1600834509) AS end
    SELECT  time_stamp,
            uniqMerge(value) AS value
    FROM event_total_user_by_source_ts
    WHERE (tenant_id = 1)
            AND (source_id = 'ADR-1htXpdVcfiprEmhWgs3GSEd13qF')
            AND (time_stamp >= start)
            AND (time_stamp <= end)
    GROUP BY tenant_id,
             source_id,
             time_stamp
) AS t2
ON t1.time_stamp = t2.time_stamp
ORDER BY t1.time_stamp

----------------------------------------total user by source ts table => can not refactor
show create table event_total_user_by_source_ts_mv;

SELECT
    toStartOfFifteenMinutes(at) AS time_stamp,
    tenant_id,
    str_properties.vals[indexOf(str_properties.keys, 'source.scope')] AS source_id,
    uniqState(anonymous_id) AS value
FROM eventify.events
GROUP BY
    time_stamp,
    tenant_id,
    source_id
 ----------------------------------Event summary by anonymous.= > can not refactor.

 SELECT
    tenant_id,
    anonymous_id,
    event_name,
    count(event_name) AS count,
    min(at) AS first_time,
    max(at) AS last_time
FROM events
WHERE (tenant_id = 1) AND (anonymous_id = 'a1')
GROUP BY
    tenant_id,
    anonymous_id,
    event_name
ORDER BY event_name ASC
;

----------------------------------------------get user activity => can not refactor.
SELECT
    event_id,
    tenant_id,
    anonymous_id,
    toUnixTimestamp(toStartOfDay(at)) AS date_unix,
    toUnixTimestamp(at) AS time_unix,
    `str_properties.vals`[indexOf(`str_properties.keys`, 'session_id')] AS session_id,
    event_name,
    arrayZip(`identity.keys`, `identity.vals`) AS identities,
    arrayFilter(x -> ((x.1) IN []), arrayZip(`str_properties.keys`, `str_properties.vals`)) AS str_pros,
    arrayFilter(x -> ((x.1) IN []), arrayZip(`num_properties.keys`, `num_properties.vals`)) AS num_pros,
    arrayFilter(x -> ((x.1) IN []), arrayZip(`arr_properties.keys`, `arr_properties.vals`)) AS arr_pros

FROM events
WHERE (tenant_id = 1)
    AND (anonymous_id = 'a1')
ORDER BY at DESC
;

select * from events where anonymous_id = 'a0';

select *
from
     (
      select tenant_id,
             anonymous_id,
             event_id,
             `str_properties.vals`[indexOf(`str_properties.keys`, 'session_id')] AS session_id,
             event_name,
            `identity.keys`,
             `identity.vals`,
             `str_properties.keys`,
             `str_properties.vals`
      from events array join identity array join str_properties
      where anonymous_id = 'a0'
         ) array join [1,2] as abc

-------------------------- get total user by segment timeseries => Not refactor

SELECT arrayZip(time_stamps, total_users) AS data_point
FROM
(
    SELECT
        groupArray(toUnixTimestamp(time_stamp)) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
        WITH
            60*60*24 AS delta,
            {start_time} AS start_unix,
            {end_time} AS end_unix,
            FROM_UNIXTIME(start_unix) AS start,
            FROM_UNIXTIME(end_unix) AS end,
            ceil((end_unix - start_unix) / delta) AS n
        SELECT toStartOfDay(start) + number * delta AS time_stamp
        FROM system.numbers
        LIMIT n
    ) AS t1
    LEFT JOIN
    (
        SELECT
            tenant_id,
            segment_id,
            round(avg(length(users))) AS total_user,
            toStartOfDay(at) AS time_stamp
        FROM
        (
            WITH
                FROM_UNIXTIME(1600794000) AS start,
                FROM_UNIXTIME(1600834509) AS end
            SELECT DISTINCT *
            FROM segment_users
            WHERE (tenant_id = 1)
                AND (segment_id = 's1')
                AND (at >= start)
                AND (at <= end)
        )
        GROUP BY
            tenant_id,
            segment_id,
            time_stamp
    ) AS t2 ON t1.time_stamp = t2.time_stamp
)
;


----------------------------------GET histogram data => can not refactor.
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
        num_pros_vals[indexOf(num_pros_keys, 'total_order')] AS total
    FROM user_profile_final_v
    WHERE tenant_id = 1
) AS p ON (s.tenant_id = p.tenant_id) AND (s.users = p.anonymous_id)


---------------------------------- GET sample profile by segment
        SELECT
            tenant_id,
            anonymous_id,
            arrayZip(identity_keys, identity_vals) AS identities,
            arrayFilter(x -> ((x.1) IN :str_pros_filter), arrayZip(str_pros_keys, str_pros_vals)) AS str_pros,
            arrayFilter(x -> ((x.1) IN :num_pros_filter), arrayZip(num_pros_keys, num_pros_vals)) AS num_pros,
            arrayFilter(x -> ((x.1) IN :arr_pros_filter), arrayZip(arr_pros_keys, arr_pros_vals)) AS arr_pros,
            at
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
                    users
                FROM segment_users_final_v
                WHERE (tenant_id = :tenant_id) AND (segment_id = :segment_id)
            )
            ARRAY JOIN users
            LIMIT :limit
        ) AS s
        INNER JOIN user_profile_final_v AS p ON (s.tenant_id = p.tenant_id) AND (s.users = p.anonymous_id)
;


-------------------------------  Tam thoi chua refactor dc j
