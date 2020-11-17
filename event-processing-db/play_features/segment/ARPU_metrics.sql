-- Average revenue per user.

SELECT
    ['total_customer'] as metric_key,
    [length(users)] as metric_value
FROM
(
    SELECT
        argMaxMerge(users) as users
    FROM segment_users_final
    WHERE (tenant_id = 1) AND (segment_id = 's1')
    GROUP BY
        tenant_id,
        segment_id
)
;

select * from segment_users_final_v;



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
        WHERE tenant_id = 0 AND segment_id = 's1'
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
        WHERE tenant_id = 0
        GROUP BY
                 tenant_id,
                 anonymous_id
    )
) AS pf ON (sf.tenant_id = pf.tenant_id) AND (sf.users = pf.anonymous_id)
WHERE v >= 0;


select * from user_profile_final_v where tenant_id = 0 and anonymous_id = 'a1';


select FROM_UNIXTIME(1603700488);


select * from user_profile_final_v where anonymous_id = 'DBsHkSvfwAKWTiy3AhvyarCMxaI';

select * from segment_users where segment_id = '1j5Vdv1OC6kwfDdQ6OPzSEJLvgB';


select * from segment_users;
select * from user_segments;


select * from user_profile_final_v where anonymous_id ='DCQ5YdxiIt4fs4fbxZq9Btn3Z9B';

select * from user_profile_final_v


['DCQ5YdxiIt4fs4fbxZq9Btn3Z9B','DCQuuK1t79CV8BxYtIYxbNYA6WD']


;


select * from segment_users where segment_id = 'DCQ5YdxiIt4fs4fbxZq9Btn3Z9B';


select * from events where anonymous_id = 'DCRKwA9CWiJOop6tbTCnc9BFs7J';










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
                WHERE tenant_id =1 AND segment_id = 's1'
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
                 arr_vals[indexOf(arr_keys, 'revenue')] AS channels
            FROM
            (
                SELECT tenant_id,
                       anonymous_id,
                       argMaxMerge(arr_properties.keys) AS arr_keys,
                       argMaxMerge(arr_properties.vals) AS arr_vals
                FROM user_profile_final
                WHERE tenant_id = %(tenant_id)s
                GROUP BY tenant_id,
                         anonymous_id
            )
        ) AS pf
        ON sf.tenant_id = pf.tenant_id AND sf.users = pf.anonymous_id
    ) ARRAY JOIN channels
    GROUP BY channels
)
;