drop table test;
create table test(
    user String,
    time UInt16,
    number UInt16
)
ENGINE = Log()
;

insert into test values
('u1',1,1),('u1', 3, 2),

('u2',5,3),('u2', 6, 2),('u2', 7, 3),
('u3', 10,2);

insert into test values ('u2',4,1);
insert into test values ('u3',11,1);
insert into test values ('u4',15,1);
insert into test values ('u4',16,2);
insert into test values ('u4',17,3);
insert into test values ('u4',18,4);

insert into test values ('u1',4,4);

insert into test values ('u4',20,1);
insert into test values ('u4',22,2);
insert into test values ('u4',24,3);

-----------------------------------
SELECT * FROM test order by user, time;


------------------- performed event 1 equal 1
select * from (
select user,
       sequenceCount('(?1).*(?2)')(time, number=1, number !=1) as count
from test
group by user)
where count = 1;



------------------- performed event 1 equal 1
select user,
       sequenceCount('(?1)')(time, number=1,0)
from test
group by user;

-------------------
select user,
       sequenceCount('(?1).*(?2)')(time, number=1, number=2)
from test
group by user;

-------------------
with (select 1) as one
select user,
       sequenceCount('(?1)(?2)(?3)')(time, number=1, number=2,number=3)
from test
group by user;

-----------------------------------------------------