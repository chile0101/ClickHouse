show tables;


select *
from user_profile_final_v;

SELECT
    groupArray(cate) AS {METRIC_KEY},
    groupArray(count) AS {METRIC_VALUE}
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
            str_vals[indexOf(str_keys, :pros_key)] AS cate
        FROM
        (
            SELECT
                tenant_id,
                anonymous_id,
                argMaxMerge(str_properties.keys) AS str_keys,
                argMaxMerge(str_properties.vals) AS str_vals
            FROM user_profile_final
            WHERE tenant_id = :tenant_id
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
);



1,a0,"['google','mailchimp']"
1,a6,"['facebook','google']"
1,a7,"['facebook','onesignal']"
1,a1,"['google','mailchimp']"
1,a3,"['facebook','onesignal']"
1,a8,"['google','mailchimp']"
1,a5,"['facebook','onesignal']"
1,a4,"['google','mailchimp']"
1,a9,"['facebook','onesignal']"
1,a2,"['facebook','onesignal']"


google, facebook, mailchim, onsignal
2, 2, 2



SELECT
    groupArray(channels) AS keys,
    groupArray(count) AS vals
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
                WHERE tenant_id = 1 AND segment_id = 's1'
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
            FROM (
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

truncate table segment_users;
truncate table segment_users_final;
