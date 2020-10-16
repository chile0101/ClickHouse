show tables;

select * from segment_users_final_v where tenant_id = 1 and segment_id = 's1';


insert into segment_users
with (
select groupArray(anonymous)
from
(select concat('a',toString(number)) as anonymous
from system.numbers limit 100)) as user_arr
select
1 as tenant_id,
's1' as segment_id,
user_arr as users,
now() as at
;


