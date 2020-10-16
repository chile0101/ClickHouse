faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s1", "state": "in", "user":"a1", "at":"1599320070"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s1", "state": "in", "user":"a2", "at":"1599320071"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s1", "state": "out", "user":"a1", "at":"1599320072"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s1", "state": "in", "user":"a2", "at":"1599320073"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s1", "state": "in", "user":"a3", "at":"1599320074"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s1", "state": "in", "user":"a4", "at":"1599320075"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s1", "state": "in", "user":"a5", "at":"1599320076"}'


faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s1", "state": "in", "user":"a6", "at":"1599320077"}'


faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s2", "state": "in", "user":"a1", "at":"1599320078"}'


faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s2", "state": "in", "user":"a2", "at":"1599320079"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s2", "state": "in", "user":"a3", "at":"1599320080"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s2", "state": "in", "user":"a4", "at":"1599320081"}'


faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s2", "state": "in", "user":"a5", "at":"1599320082"}'


faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":10, "segment":"s2", "state": "in", "user":"a6", "at":"1599320083"}'


select * from user_segments order by at desc ;
select * from segment_users order by at;

truncate table user_segments;
truncate table segment_users;

select toUnixTimestamp(now());

