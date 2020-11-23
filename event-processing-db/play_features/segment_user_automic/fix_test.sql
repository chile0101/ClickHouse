1kV6ImrTYzKGcdbV2sIJecFyHWV

1kV6ImrTYzKGcdbV2sIJecFyHWV


1kV6ImrTYzKGcdbV2sIJecFyHWV


select * from segment_user;
select * from segment_size where segment_id = '1kV6ImrTYzKGcdbV2sIJecFyHWV';





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
    SELECT tenant_id,
           segment_id,
           user
    FROM segment_user_final_v
    WHERE tenant_id = 1 AND segment_id ='1kV6ImrTYzKGcdbV2sIJecFyHWV' AND status = 1
    LIMIT 9
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
        WHERE tenant_id = 1 AND str_key IN ['first_name', 'last_name', 'city', 'country', 'gender']
        GROUP BY tenant_id, anonymous_id
    ) AS ps
    JOIN
    (
        SELECT tenant_id,
               anonymous_id,
               groupArray((num_key, num_val)) AS num_pros,
               max(at)                        AS at
        FROM profile_num_final_v
        WHERE tenant_id = 1 AND num_key IN  ['revenue']
        GROUP BY tenant_id, anonymous_id
    ) AS pn
    ON ps.tenant_id = pn.tenant_id AND ps.anonymous_id = pn.anonymous_id
) AS p
ON (s.tenant_id = p.tenant_id) AND (s.user = p.anonymous_id);
----------------------------------------


SELECT
    pf.cate as key,
    count() AS value
FROM
(
    SELECT tenant_id,
           segment_id,
           user
    FROM segment_user_final_v
    WHERE tenant_id =1 AND segment_id = '1kV6ImrTYzKGcdbV2sIJecFyHWV' AND status = 1
) AS sf
INNER JOIN
(
    SELECT tenant_id,
           anonymous_id,
           str_val AS cate
    FROM profile_str_final_v
    WHERE tenant_id = 1 AND str_key = 'acquisition_type'
) AS pf ON (sf.tenant_id = pf.tenant_id) AND (sf.user = pf.anonymous_id)
GROUP BY
    tenant_id,
    segment_id,
    cate
ORDER BY
    value DESC,
    key ASC;


select * from profile_str_final_v where anonymous_id = 'DEPfjgWU8bsBp4DDKxWBB9Bl2IN';

------------------------------P trait


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
          tenant_id = 1
          and anonymous_id = 'DEPfjgWU8bsBp4DDKxWBB9Bl2IN'
          and str_key IN ['rfm30', 'rfm60', 'frequency90']
    group by tenant_id, anonymous_id
) as ps
 full join
(
    select tenant_id,
           anonymous_id,
           groupArray((num_key, num_val)) as num_pros,
           max(at) as at
    from profile_num_final_v
    where
          tenant_id = 1
          and anonymous_id = 'DEPfjgWU8bsBp4DDKxWBB9Bl2IN'
          and num_key IN  ['revenue', 'total_order', 'recency', 'frequency1', 'frequency30', 'frequency60', 'frequency90', 'monetary1', 'monetary30', 'monetary60', 'monetary90']
    group by tenant_id, anonymous_id
) as pn
on ps.tenant_id = pn.tenant_id and ps.anonymous_id = pn.anonymous_id;

{'tenant_id': 1, 'anonymous_id': 'DEPfjgWU8bsBp4DDKxWBB9Bl2IN', 'str_pros_filter': ['rfm30', 'rfm60', 'frequency90'], 'num_pros_filter': ['revenue', 'total_order', 'recency', 'frequency1', 'frequency30', 'frequency60', 'frequency90', 'monetary1', 'monetary30', 'monetary60', 'monetary90']}

------------------------------revenue in user_profile != get segment sample

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
    SELECT tenant_id,
           segment_id,
           user
    FROM segment_user_final_v
    WHERE tenant_id = 1 AND segment_id ='1kV6ImrTYzKGcdbV2sIJecFyHWV' AND status = 1
    LIMIT 9
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
        WHERE tenant_id = 1 AND str_key IN ['first_name', 'last_name', 'city', 'country', 'gender'], 'num_pros_filter': ['revenue']
        GROUP BY tenant_id, anonymous_id
    ) AS ps
    JOIN
    (
        SELECT tenant_id,
               anonymous_id,
               groupArray((num_key, num_val)) AS num_pros,
               max(at)                        AS at
        FROM profile_num_final_v
        WHERE tenant_id = 1 AND num_key IN ['revenue']
        GROUP BY tenant_id, anonymous_id
    ) AS pn
    ON ps.tenant_id = pn.tenant_id AND ps.anonymous_id = pn.anonymous_id
) AS p
ON (s.tenant_id = p.tenant_id) AND (s.user = p.anonymous_id)
