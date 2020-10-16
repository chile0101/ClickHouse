SELECT
    tenant_id,
    anonymous_id,
    arrayZip(identity_keys, identity_vals) as identities,
    arrayFilter(x -> ((x.1) IN :str_pros_filter), arrayZip(str_pros_keys, str_pros_vals)) AS str_pros,
    arrayFilter(x -> ((x.1) IN :num_pros_filter), arrayZip(num_pros_keys, num_pros_vals)) AS num_pros,
    [] AS arr_pros,
    at
FROM user_profile_final_v
WHERE tenant_id = :tenant_id
LIMIT :limit

;


select
       tenant_id,
       segment_id,
       anonymous_id,
       arrayZip(identity_keys, identity_vals) as identities,
       arrayFilter(x -> ((x.1) IN []), arrayZip(str_pros_keys, str_pros_vals)) AS str_pros,
    arrayFilter(x -> ((x.1) IN []), arrayZip(num_pros_keys, num_pros_vals)) AS num_pros,
       [] as arr_pros,
       at
from (
      select tenant_id,
             segment_id,
             users
      from (
               select tenant_id,
                      segment_id,
                      users
               from segment_users_final_v
               where tenant_id = 1
                 and segment_id = 's1'
               ) array join users
      limit 3
         ) as s
INNER JOIN user_profile_final_v  as p
ON (s.tenant_id = p.tenant_id and s.users = p.anonymous_id)
;



-- s1, t1 -> n[a1,a2,a3...a9]


-- with (
--     select
--            users
--     from (
--              select
--                     users
--              from segment_users_final_v
--              where tenant_id = 1
--                and segment_id = 's1'
--              )
--              array join users
--     limit 3
-- ) as segment_users
-- select *
-- from user_profile_final
-- where anonymous_id in segment_users
-- ;


