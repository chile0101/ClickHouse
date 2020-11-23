show tables;
select * from events_campaign;

insert into events_campaign
    select * from eventify.events_campaign;

------------------------------------
SELECT tenant_id,
       anonymous_id,
       event_name,
        event_name_type,
       utm_campaign,
       utm_content,
       utm_source,
       total_value,
       time_stamp,
       gender,
       location_city,
       device
FROM
(
    SELECT
        tenant_id,
        anonymous_id,
        event_name,
        event_name_type,
        utm_campaign,
        utm_content,
        utm_source,
        `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] AS total_value,
        toUnixTimestamp(toStartOfDay(at)) AS time_stamp
    FROM events_campaign
    WHERE tenant_id = 1
      AND utm_campaign = 'ARPU Increase Experiment'
      AND toUnixTimestamp(at) >= 0
      AND toUnixTimestamp(at) <= 9999999999
) AS ec
INNER JOIN
(
    SELECT
           tenant_id,
           anonymous_id,
           str_vals[indexOf(str_keys, 'gender')] AS gender,
          str_vals[indexOf(str_keys, 'city')] AS location_city,
           str_vals[indexOf(str_keys, 'device_platform')] AS device
    FROM
    (
        SELECT tenant_id,
               anonymous_id,
               groupArray(str_key) AS str_keys,
               groupArray(str_val) AS str_vals
        FROM profile_str_final_v
        WHERE tenant_id = 1
              AND  anonymous_id IN (SELECT anonymous_id FROM events_campaign WHERE tenant_id = 1
                                      AND utm_campaign = 'ARPU Increase Experiment'
                                      AND toUnixTimestamp(at) >= 0
                                      AND toUnixTimestamp(at) <= 9999999999 )
              AND str_key IN ('gender', 'city', 'device_platform')  -- execution: 341 ms , 281ms
         GROUP BY tenant_id, anonymous_id
    )
) AS pf
ON ec.tenant_id = pf.tenant_id AND ec.anonymous_id = pf.anonymous_id;






--1 s 337 ms

select
       tenant_id,
       anonymous_id,
       gr(str_key),
       groupArray(str_val)
from
    (
select tenant_id,
       anonymous_id,
       str_key,
       str_val
from profile_str_final_v
where tenant_id = 1
  and  anonymous_id in (select anonymous_id from events_campaign WHERE tenant_id = 1
                          AND utm_campaign = 'ARPU Increase Experiment'
                          AND toUnixTimestamp(at) >= 0
                          AND toUnixTimestamp(at) <= 9999999999 )
      and str_key in ('gender', 'city', 'device_platform')  -- execution: 341 ms , 281ms
) group by tenant_id, anonymous_id
;




SELECT
       tenant_id,
       anonymous_id,
       str_vals[indexOf(str_keys, 'gender')] AS gender,
      str_vals[indexOf(str_keys, 'city')] AS city,
       str_vals[indexOf(str_keys, 'device_platform')] AS device
FROM (
SELECT tenant_id,
       anonymous_id,
        groupArray(str_key) AS str_keys,
       groupArray(str_val) AS str_vals
FROM profile_str_final_v
WHERE tenant_id = 1
  AND  anonymous_id IN (SELECT anonymous_id FROM events_campaign WHERE tenant_id = 1
                          AND utm_campaign = 'ARPU Increase Experiment'
                          AND toUnixTimestamp(at) >= 0
                          AND toUnixTimestamp(at) <= 9999999999 )
      AND str_key IN ('gender', 'city', 'device_platform')  -- execution: 341 ms , 281ms
 GROUP BY tenant_id, anonymous_id
)
;
-----------------------GET SAMPLE
SELECT
    tenant_id,
    anonymous_id,
    [] AS identities,
    str_pros,
    num_pros,
    [] arr_pros,
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
        WHERE (tenant_id = 1) AND (segment_id = '1kAjXcKs1krG9emjry9gcXLau6c')
    )
    ARRAY JOIN users
    LIMIT 100
) AS s
INNER JOIN
(
select tenant_id,
       anonymous_id,
       str_pros,
       num_pros,
       ps.at > pn.at ? ps.at : pn.at as at
from
(
    select tenant_id,
           anonymous_id,
           groupArray((str_key, str_val)) as str_pros,
           max(at) as at
    from profile_str_final_v
    where tenant_id = 2
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
on ps.tenant_id = pn.tenant_id and ps.anonymous_id = pn.anonymous_id
)
AS p
ON (s.tenant_id = p.tenant_id) AND (s.users = p.anonymous_id);





select tenant_id,
       anonymous_id,
       str_pros,
       num_pros,
       ps.at > pn.at ? ps.at : pn.at as at
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
        WHERE (tenant_id = 2) AND (segment_id = '1kAjXcKs1krG9emjry9gcXLau6c')
    )
    ARRAY JOIN users
    LIMIT 9;


select * from segment_users_final_v;


-----------------------------SEGMENT GET TOTAL USER


SELECT
    count() as total_user_have_conversion
FROM
(
 SELECT tenant_id, users
 FROM
 (
    SELECT
        tenant_id,
        argMaxMerge(users) as users
    FROM segment_users_final
    WHERE (tenant_id = :tenant_id) AND (segment_id = :segment_id)
    GROUP BY
        tenant_id,
        segment_id
 ) ARRAY JOIN users
)  as sf
JOIN
(
SELECT
    tenant_id,
    anonymous_id,
    arrayExists(key -> key == 'revenue', num_keys) as have_conversion
FROM
    (
        select tenant_id,
               anonymous_id,
               groupArray(num_key) as num_keys
        from profile_num
        where tenant_id = 1
        group by tenant_id, anonymous_id
    )
    WHERE have_conversion != 0

) as pf
ON sf.tenant_id = pf.tenant_id and sf.users =pf.anonymous_id;


-------------------------------segment report aggregate



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
        WHERE tenant_id = :tenant_id AND segment_id = :segment_id
        GROUP BY
            tenant_id,
            segment_id
    )
    ARRAY JOIN users
) AS sf
INNER JOIN
(
    SELECT tenant_id,
           anonymous_id,
           str_val AS cate
    FROM profile_str_final_v
    WHERE tenant_id = 1 AND str_key = 'gender'
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
;

select tenant_id,
       anonymous_id,
       str_val as cate
from profile_str_final_v
where tenant_id = 1 and str_key = 'gender'


---------------------------SEGMENT NUM PROS KEY


SELECT
    keys[n] AS key,
    values[n] AS value
FROM
(
    SELECT
        ['{MAX}', '{AVG}', '{MED}', '{MIN}'] AS keys,
        [maxOrDefault(v), avgOrDefault(v), medianOrDefault(v), minOrDefault(v)] AS values
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
            WHERE tenant_id = 1 AND segment_id = '1jsIYxjbtyLb37ZPWfVH9gPNzW7'
            GROUP BY
                tenant_id,
                segment_id
        )
        ARRAY JOIN users
    ) AS sf
    INNER JOIN
    (
        SELECT tenant_id,
               anonymous_id,
               num_val AS v
        FROM profile_num_final_v
        WHERE tenant_id = 1 AND num_key = 'revenue'
    ) AS pf ON (sf.tenant_id = pf.tenant_id) AND (sf.users = pf.anonymous_id)
    WHERE v >= 0
) ARRAY JOIN arrayEnumerate(keys) AS n;


----------------------------SEGMENT GET DAY SINCE LAST ORDER

SELECT
    ['{MAX}', '{AVG}', '{MED}', '{MIN}'] AS {METRIC_KEY},
    [toFloat64(maxOrDefault(days)), avgOrDefault(days), medianOrDefault(days), toFloat64(minOrDefault(days))] AS {METRIC_VALUE}
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
            WHERE tenant_id = :tenant_id AND segment_id = :segment_id
            GROUP BY
                tenant_id,
                segment_id
        )
        ARRAY JOIN users
    ) AS sf
    INNER JOIN
    (
        SELECT  tenant_id,
                anonymous_id,
                num_val AS v
        FROM profile_num_final_v
        WHERE tenant_id = 1 AND num_key = 'revenue'
    ) AS pf ON (sf.users = pf.anonymous_id) AND (sf.tenant_id = pf.tenant_id)
    WHERE t > 0
)

-----------------------_GET TOTAL REVENUE


SELECT
    sum(v) as total_revenue
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
        WHERE tenant_id = :tenant_id AND segment_id = :segment_id
        GROUP BY
            tenant_id,
            segment_id
    )
    ARRAY JOIN users
) AS sf
INNER JOIN
(
    SELECT tenant_id,
           anonymous_id,
           num_val AS v
    FROM profile_num_final_v
    WHERE tenant_id = :tenant_id AND num_key = :pros_key
) AS pf ON (sf.tenant_id = pf.tenant_id) AND (sf.users = pf.anonymous_id)
WHERE v >= 0

------------------------_GET HISTOGRAM


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
        WHERE (tenant_id = :tenant_id) AND (segment_id = :segment_id)
    ) AS s
    INNER JOIN
    (
        SELECT
            tenant_id,
            anonymous_id,
            num_pros_vals[indexOf(num_pros_keys, :pros_key)] AS total
        FROM user_profile_final_v
        WHERE tenant_id = :tenant_id
    ) AS p
    ON (s.tenant_id = p.tenant_id) AND (s.users = p.anonymous_id);






select * from events_campaign order by at desc ;







SELECT
    tenant_id,
    anonymous_id,
    [] AS identities,
    str_pros,
    num_pros,
    [] AS arr_pros,
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
INNER JOIN
(
    SELECT tenant_id,
           anonymous_id,
           str_pros,
           num_pros,
           ps.at > pn.at ? ps.at : pn.at AS at
    FROM
    (
        SELECT tenant_id,
               anonymous_id,
               groupArray((str_key, str_val)) AS str_pros,
               max(at) AS at
        FROM profile_str_final_v
        WHERE tenant_id = :tenant_id AND str_key IN :str_pros_filter
        GROUP BY tenant_id, anonymous_id
    ) AS ps
    JOIN
    (
        SELECT tenant_id,
               anonymous_id,
               groupArray((num_key, num_val)) AS num_pros,
               max(at)                        AS at
        FROM profile_num_final_v
        WHERE tenant_id = :tenant_id AND num_key IN :num_pros_filter
        GROUP BY tenant_id, anonymous_id
    ) AS pn
    ON ps.tenant_id = pn.tenant_id AND ps.anonymous_id = pn.anonymous_id
) AS p
ON (s.tenant_id = p.tenant_id) AND (s.users = p.anonymous_id)
;




SELECT tenant_id,
       anonymous_id,
       groupArray((num_key, num_val)) AS num_pros,
       max(at)                        AS at
FROM profile_num_final_v
WHERE tenant_id = :tenant_id
  AND num_key IN :num_pros_filter
GROUP BY tenant_id, anonymous_id

------------------___GET LASTEST PROFILE


select tenant_id,
       anonymous_id,
       [] as identities,
       str_pros,
       num_pros,
       [] as arr_pros,
       ps.at > pn.at ? ps.at : pn.at as at
FROM
(
    select tenant_id,
           anonymous_id,
           groupArray((str_key, str_val)) as str_pros,
           max(at) as at
    from profile_str_final_v
    where
          tenant_id = :tenant_id
          and anonymous_id = :anonymous_id
          and str_key IN :str_pros_filter
    group by tenant_id, anonymous_id
) as ps
    join
(
    select tenant_id,
           anonymous_id,
           groupArray((num_key, num_val)) as num_pros,
           max(at) as at
    from profile_num_final_v
    where
          tenant_id = :tenant_id
          and anonymous_id = :anonymous_id
          and num_key IN :num_pros_filter
    group by tenant_id, anonymous_id
) as pn
on ps.tenant_id = pn.tenant_id and ps.anonymous_id = pn.anonymous_id