select * from segment_users;


select *
from segment_users
where segment_id = 's1';

-- get segment size
-- select *
-- from segment_users_final
-- where segment_id = 's1'
--     and at_final >= '2020-09-25 00:00:00'
--     and at_final <= '2020-10-05 00:00:00'
-- ;
-- if segment user final haven't => get near start from segment_users
SELECT length(users) as segment_size
FROM segment_users
WHERE segment_id = 's1' AND at <= '2020-09-03 00:00:00'
ORDER BY at DESC
LIMIT 1;



SELECT
       tenant_id,
       segment_id,
       length(users) AS num_of_users
FROM segment_users;



