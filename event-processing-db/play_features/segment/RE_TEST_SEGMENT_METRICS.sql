SELECT
    groupArray(channels) AS metric_key,
    groupArray(count) AS metric_value
FROM
(
    SELECT
           channels,
           count(anonymous_id) as count
    FROM
    (
        SELECT tenant_id,
               segment_id,
               anonymous_id,
               channels
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
                WHERE tenant_id = 0 AND segment_id = 's1'
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
                 arr_vals[indexOf(arr_keys, 'channels')] AS channels
            FROM
            (
                SELECT tenant_id,
                       anonymous_id,
                       argMaxMerge(arr_properties.keys) AS arr_keys,
                       argMaxMerge(arr_properties.vals) AS arr_vals
                FROM user_profile_final
                WHERE tenant_id = 1
                GROUP BY tenant_id,
                         anonymous_id
            )
        ) AS pf
        ON sf.tenant_id = pf.tenant_id AND sf.users = pf.anonymous_id
    ) ARRAY JOIN channels
    GROUP BY channels
)
;

select * from segment_users_final_v where tenant_id = 0 and segment_id = 's1';
select * from user_profile_final_v where anonymous_id = 'a1';

select * from user_profile_final_v;


select count(*) from events;
527558

select * from user_profile_final_v order by at desc ;