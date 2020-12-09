SELECT if(ps.tenant_id != 0, ps.tenant_id, pn.tenant_id) AS tenant_id,
        if(ps.anonymous_id != NULL, ps.anonymous_id, pn.anonymous_id) AS anonymous_id,
        [] AS identities,
        str_pros,
        num_pros,
        [] AS arr_pros,
        ps.at > pn.at ? ps.at : pn.at AS at
FROM
(
    SELECT tenant_id,
           anonymous_id,
           groupArray((str_key, str_val)) AS str_pros,
           max(at) AS at
    FROM profile_str_final_v
    WHERE tenant_id = 1
        AND str_key IN ['first_name', 'last_name', 'city', 'country', 'gender']
        AND anonymous_id IN (
                SELECT user
                FROM segment_user_final_v
                WHERE tenant_id =1 AND segment_id = '1l0Q1AZJlZl63Ew1pOodfzBm7qW' AND status = 1
        )
    GROUP BY tenant_id, anonymous_id
) AS ps
 FULL JOIN
 (
    SELECT tenant_id,
           anonymous_id,
           groupArray((num_key, num_val)) AS num_pros,
           max(at)                        AS at
    FROM profile_num_final_v
    WHERE tenant_id = 1
      AND num_key IN ['revenue']
      AND anonymous_id in (
                SELECT user
                FROM segment_user_final_v
                WHERE tenant_id = 1 AND segment_id ='1l0Q1AZJlZl63Ew1pOodfzBm7qW' AND status = 1
        )
    GROUP BY tenant_id, anonymous_id
) AS pn
ON ps.tenant_id = pn.tenant_id AND ps.anonymous_id = pn.anonymous_id

;


2020-12-02 13:57:44,532 INFO sqlalchemy.engine.base.Engine {'tenant_id': 1, 'str_pros_filter': ['first_name', 'last_name', 'city', 'country', 'gender'], 'segment_id': '1l0Q1AZJlZl63Ew1pOodfzBm7qW', 'num_pros_filter': ['revenue'], 'limit': 9}
