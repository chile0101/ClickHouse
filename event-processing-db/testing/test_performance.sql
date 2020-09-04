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
    GROUP BY
        tenant_id,
        anonymous_id
)
WHERE anonymous_id IN anonymous_id_filter_by_segment_v
;



CREATE VIEW anonymous_id_filter_by_segment_v AS
SELECT DISTINCT arrayJoin(users)
FROM
(
    SELECT users
    FROM
    (
        SELECT argMaxMerge(users) AS users
        FROM segment_users_final
        GROUP BY
            tenant_id,
            segment_id
    )
)
;
--------------------------------- chua dc


SELECT
    tenant_id,
    segment_id,
    now() AS time_stamp,
    'gender' AS metric_name,
    groupArray(cate) AS `metrics_agg.keys`,
    groupArray(count) AS `metrics_agg.vals`
FROM
(
    SELECT
        sf.tenant_id,
        sf.segment_id,
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
GROUP BY
    tenant_id,
    segment_id
;


-----------------------------------------------------------
SELECT
    tenant_id,
    segment_id,
    now() AS time_stamp,
    'gender' AS metric_name,
    groupArray(cate) AS `metrics_agg.keys`,
    groupArray(count) AS `metrics_agg.vals`
FROM
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
        GROUP BY
            tenant_id,
            anonymous_id
    )
) AS pf INNER JOIN
(
    SELECT sf.tenant_id,
           sf.segment_id,
           pf.cate,
           count() AS count
    FROM
    (
         SELECT tenant_id,
                segment_id,
                users
         FROM (
                  SELECT tenant_id,
                         segment_id,
                         argMaxMerge(users) AS users
                  FROM segment_users_final
                  GROUP BY tenant_id,
                           segment_id
                  )
                  ARRAY JOIN users
    ) AS sf
)
ON (sf.users = pf.anonymous_id) AND (sf.tenant_id = pf.tenant_id)
    WHERE cate != ''
    GROUP BY
        tenant_id,
        segment_id,
        cate
    ORDER BY
        count DESC,
        cate ASC
)
GROUP BY
    tenant_id,
    segment_id
;






--
SELECT
    tenant_id,
    anonymous_id,

FROM
(
    SELECT
        tenant_id,
        anonymous_id,
        argMaxMerge(str_properties.keys) AS str_keys,
        argMaxMerge(str_properties.vals) AS str_vals
    FROM user_profile_final
    GROUP BY
        tenant_id,
        anonymous_id
)














show tables ;

insert into segment_users values
(1, 's1', ['a1'], now()),
(1, 's2', ['a1'], now()),
;

insert into user_segments values
(1, 'a1', ['s1','s2'], '2020-01-01 00:00:01'),
(1, 'a2', ['s1','s2'], '2020-01-01 00:00:01'),
(1, 'a3', ['s1','s2'], '2020-01-01 00:00:01'),
;

select *
from user_segments_final;

select
       tenant_id,
       segment_id,
       argMaxMerge(users),
       max(at_final)
from segment_users_final
group by tenant_id, segment_id;

select * from segment_users;





SELECT user_profile_final_v.str_pros_keys AS user_profile_final_v_str_pros_keys,
       user_profile_final_v.str_pros_vals AS user_profile_final_v_str_pros_vals,
       user_profile_final_v.num_pros_keys AS user_profile_final_v_num_pros_keys,
       user_profile_final_v.num_pros_vals AS user_profile_final_v_num_pros_vals,
       user_profile_final_v.arr_pros_keys AS user_profile_final_v_arr_pros_keys,
       user_profile_final_v.arr_pros_vals AS user_profile_final_v_arr_pros_vals,
       user_profile_final_v.anonymous_id AS user_profile_final_v_anonymous_id,
       user_profile_final_v.tenant_id AS user_profile_final_v_tenant_id,
       user_profile_final_v.identity_keys AS user_profile_final_v_identity_keys,
       user_profile_final_v.identity_vals AS user_profile_final_v_identity_vals,
       user_profile_final_v.at AS user_profile_final_v_at
FROM user_profile_final_v
WHERE user_profile_final_v.tenant_id = 1
    AND user_profile_final_v.anonymous_id = 'a12345'
;
-- 0.017

SELECT * FROM (
SELECT anonymous_id,
      tenant_id,
      argMaxMerge(identity.keys)       AS identity_keys,
      argMaxMerge(identity.vals)       AS identity_vals,
      argMaxMerge(str_properties.keys) AS str_pros_keys,
      argMaxMerge(str_properties.vals) AS str_pros_vals,
      argMaxMerge(num_properties.keys) AS num_pros_keys,
      argMaxMerge(num_properties.vals) AS num_pros_vals,
      argMaxMerge(arr_properties.keys) AS arr_pros_keys,
      argMaxMerge(arr_properties.vals) AS arr_pros_vals,
      max(at_final)                    AS at
FROM user_profile_final
WHERE tenant_id = 1 and anonymous_id = 'a12935'
GROUP BY  tenant_id, anonymous_id )

;
-- 0.012