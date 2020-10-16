B1: faust -A prile.workflows.segments_users_update.app worker -l info
B2: create table tables segment_agg
B3: create table MV metrics
B3: faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":1, "segment":"s1", "state": "in", "user":"a1", "at":"1598934192"}'


show tables;

select toUnixTimestamp(now());

select * from user_profile_final;




truncate table segment_users;
truncate table segment_users_final;

select * from segment_users;
select tenant_id,
       segment_id,
       argMaxMerge(users),
       max(at_final)
from segment_users_final
group by tenant_id, segment_id;

select * from segment_agg_final_v;

