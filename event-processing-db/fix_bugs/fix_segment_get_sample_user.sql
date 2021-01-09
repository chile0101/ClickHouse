1lmNXK2AGtjytQ7jB8FcV0FiN2L

select *
from segment_user
where segment_id = '1lmNXK2AGtjytQ7jB8FcV0FiN2L';


show tables;

select *
from profile_str_final_v
where anonymous_id = 'DGjaQNhRJ1mJLn8hNsnWjIHJwtc';


date range < attribution window.
all traffic -> all traffic
color chart
include direct not
select missing defautl
;



SELECT if(ps.tenant_id != 0, ps.tenant_id, pn.tenant_id)           AS tenant_id,
       if(ps.anonymous_id != '', ps.anonymous_id, pn.anonymous_id) AS anonymous_id,
       []                                                          AS identities,
       arrayFilter(x -> ((x.1) IN ['first_name', 'last_name', 'city', 'country', 'gender']), str_pros)     as str_pros,
       arrayFilter(x -> ((x.1) IN ['revenue']), num_pros)     as num_pros,
       []                                                          AS arr_pros,
       ps.at > pn.at ? ps.at : pn.at                               AS at
FROM (
         SELECT tenant_id,
                anonymous_id,
                groupArray((str_key, str_val)) AS str_pros,
                max(at)                        AS at
         FROM profile_str_final_v
         WHERE tenant_id = 1
--         AND str_key IN ['first_name', 'last_name', 'city', 'country', 'gender']
           AND anonymous_id IN (
             SELECT user
             FROM segment_user_final_v
             WHERE tenant_id = 1
               AND segment_id = '1lmNXK2AGtjytQ7jB8FcV0FiN2L'
               AND status = 1
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
--       AND num_key IN ['revenue']
           AND anonymous_id in (
             SELECT user
             FROM segment_user_final_v
             WHERE tenant_id = 1
               AND segment_id = '1lmNXK2AGtjytQ7jB8FcV0FiN2L'
               AND status = 1
         )
         GROUP BY tenant_id, anonymous_id
         ) AS pn
     ON ps.tenant_id = pn.tenant_id AND ps.anonymous_id = pn.anonymous_id
LIMIT :limit
;








SELECT if(ps.tenant_id != 0, ps.tenant_id, pn.tenant_id)           AS tenant_id,
       if(ps.anonymous_id != '', ps.anonymous_id, pn.anonymous_id) AS anonymous_id,
       []                                                          AS identities,
       arrayFilter(x -> ((x.1) IN ['first_name', 'last_name', 'city', 'country', 'gender']), str_pros)     as str_pros,
       arrayFilter(x -> ((x.1) IN ['revenue']), num_pros)     as num_pros,
       []                                                          AS arr_pros,
       ps.at > pn.at ? ps.at : pn.at                               AS at
FROM (
         SELECT tenant_id,
                anonymous_id,
                groupArray((str_key, str_val)) AS str_pros,
                max(at)                        AS at
         FROM profile_str_final_v
         WHERE tenant_id = 1
           AND anonymous_id IN (
             SELECT user
             FROM segment_user_final_v
             WHERE tenant_id = 1
               AND segment_id = '1lmNXK2AGtjytQ7jB8FcV0FiN2L'
               AND status = 1
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
           AND anonymous_id in (
             SELECT user
             FROM segment_user_final_v
             WHERE tenant_id = 1
               AND segment_id = '1lmNXK2AGtjytQ7jB8FcV0FiN2L'
               AND status = 1
         )
         GROUP BY tenant_id, anonymous_id
         ) AS pn
     ON ps.tenant_id = pn.tenant_id AND ps.anonymous_id = pn.anonymous_id
     LIMIT 10
;
