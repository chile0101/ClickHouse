show tables;

----------------------------------------------TEST EVENTS TABLE
truncate table events;
select count(*) from events;
select count(*)
from events
where event_name = 'identify'
order by at desc ;



select * from user_profile_final_v;

select toDateTime('2020-10-15 07:16', 'Asia/Ho_Chi_Minh');










-----------------------------------------------TEST USER PROFILE
truncate table user_profile;
truncate table user_profile_final;

select *
from user_profile
order by at desc;


select *
from user_profile_final_v;


--------------------------------------------