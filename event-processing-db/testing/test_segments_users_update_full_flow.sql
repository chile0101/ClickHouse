show tables ;
select toUnixTimestamp(now());

-- DIEU CHINH DATABASE
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":1, "segment":"s2", "state": "out", "user":"a3", "at":"1598085020"}'



--Test 1:  Test segment opt IN: a1 vao s1
input:
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":1, "segment":"s1", "state": "in", "user":"a1", "at":"1598085000"}'

output:
user_segments:  1,a1,['s1'],2020-08-22 08:30:00
segment_users:  1,s1,['a1'],2020-08-22 08:30:00


-- Test 2: Insert them segment opt in: a2 vao s1
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":1, "segment":"s1", "state": "in", "user":"a2", "at":"1598085001"}'


1,a2,['s1'],2020-08-22 08:30:01
1,a1,['s1'],2020-08-22 08:30:00

1,s1,['a2','a1'],2020-08-22 08:30:01
1,s1,['a1'],2020-08-22 08:30:00

-- Test: Opt In: a1 vao s2
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":1, "segment":"s2", "state": "in", "user":"a1", "at":"1598085005"}'

1,a1,['s1'],2020-08-22 08:30:00
1,a2,['s1'],2020-08-22 08:30:01
1,a1,['s2','s1'],2020-08-22 08:30:05


1,s1,['a1'],2020-08-22 08:30:00
1,s1,['a2','a1'],2020-08-22 08:30:01
1,s2,['a1'],2020-08-22 08:30:05


-- Test: Opt In: a3 vao s2
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":1, "segment":"s2", "state": "in", "user":"a3", "at":"1598085010"}'

1,a3,['s2'],2020-08-22 08:30:10
1,s2,['a3','a1'],2020-08-22 08:30:10


-- Test : OUT: a1 ra khoi s1
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":1, "segment":"s1", "state": "out", "user":"a1", "at":"1598085021"}'

1,a1,['s2'],2020-08-22 08:30:21
1,s1,['a2'],2020-08-22 08:30:21

-- Test: a10 vao s1
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":1, "segment":"s1", "state": "in", "user":"a10", "at":"1598085031"}'

1,a10,['s1'],2020-08-22 08:30:31
1,s1,"['a10','a2']",2020-08-22 08:30:31

-- Test a10 vao lai s1
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":1, "segment":"s1", "state": "in", "user":"a10", "at":"1598085040"}'

-- Thoi gian da dc update
1,a10,['s1'],2020-08-22 08:30:40
1,s1,"['a10','a2']",2020-08-22 08:30:40


-- Test : Segment opt IN/OUT outdate

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":2, "segment":"s11", "state": "in", "user":"a11", "at":"1598085040"}'

2,a11,['s11'],2020-08-22 08:30:40
2,s11,['a11'],2020-08-22 08:30:40

--outdated
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":2, "segment":"s11", "state": "out", "user":"a11", "at":"1598085038"}'



-- Test : Segment opt IN/OUT outdate

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":2, "segment":"s11", "state": "in", "user":"a12", "at":"1598085040"}'

2,a11,['s11'],2020-08-22 08:30:40
2,s11,['a11'],2020-08-22 08:30:40

-- segment opt out
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":2, "segment":"s11", "state": "out", "user":"a12", "at":"1598085038"}'
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":2, "segment":"s11", "state": "in", "user":"a13", "at":"1598085040"}'
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":2, "segment":"s11", "state": "out", "user":"a14", "at":"1598085042"}'


faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":2, "segment":"s11", "state": "out", "user":"a15", "at":"1598085045"}'


-- Test: OUTDATE -- đã fix lỗi
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":3, "segment":"s1", "state": "in", "user":"a1", "at":"1598085050"}'

3,a1,['s1'],2020-08-22 08:30:50
3,s1,['a1'],2020-08-22 08:30:50

-- try insert out date
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":3, "segment":"s1", "state": "out", "user":"a1", "at":"1598085049"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":3, "segment":"s1", "state": "out", "user":"a1", "at":"1598085045"}'


faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":3, "segment":"s1", "state": "in", "user":"a2", "at":"1598085050"}'


faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":3, "segment":"s1", "state": "in", "user":"a3", "at":"1598085050"}'


faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":3, "segment":"s1", "state": "in", "user":"a3", "at":"1598085050"}'

3,s1,"['a3','a2']",2020-08-22 08:30:50
3,a3,['s1'],2020-08-22 08:30:50

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":3, "segment":"s1", "state": "out", "user":"a3", "at":"1598085048"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":3, "segment":"s1", "state": "out", "user":"a3", "at":"1598085047"}'


faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":3, "segment":"s1", "state": "out", "user":"a3", "at":"1598085046"}'


-- RESULT
SELECT * FROM user_segments order by  tenant_id desc ,at desc ;

select * from segment_users order by tenant_id desc , at desc ;


-- DOWN
truncate table user_segments;
truncate table segment_users;
truncate table user_segments_final;
truncate table segment_users_final;

------------------------------------------------------------------------------------------------------------------------
faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":5, "segment":"s1", "state": "in", "user":"a1", "at":"1598085100"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":5, "segment":"s1", "state": "in", "user":"a2", "at":"1598085101"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":5, "segment":"s1", "state": "out", "user":"a1", "at":"1598085102"}'


faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":5, "segment":"s1", "state": "out", "user":"a2", "at":"1598085120"}'


faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":5, "segment":"s1", "state": "in", "user":"a3", "at":"1598085150"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":5, "segment":"s1", "state": "in", "user":"a4", "at":"1598085151"}'


faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":6, "segment":"s1", "state": "in", "user":"a1", "at":"1598085200"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":6, "segment":"s1", "state": "in", "user":"a2", "at":"1598085201"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":6, "segment":"s1", "state": "out", "user":"a1", "at":"1598085202"}'

faust -A prile.workflows.segments_users_update.app send "segment_opt"  '{"tenant_id":6, "segment":"s1", "state": "in", "user":"a1", "at":"1598085203"}'



select * from user_segments;
select tenant_id,
       anonymous_id,
       argMaxMerge(segments) as segments,
        max(at_final) as at
from user_segments_final
group by tenant_id, anonymous_id
order by tenant_id desc , at desc;

select * from segment_users;
select
       tenant_id,
       segment_id,
       argMaxMerge(users) as users,
        max(at_final) as at
from segment_users_final
group by tenant_id, segment_id
order by tenant_id desc , at desc ;