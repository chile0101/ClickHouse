create table profiles(
    tenant_id String,
    user_id String,
    anonymous_id String,
    identifies Array(String),
    num_properties Nested(
        keys String,
        vals Float32
    ),
    str_properties Nested(
        keys String,
        vals String
    ),
    created_at DateTime,
    expired_at DateTime,
    current UInt32
)Engine = MergeTree()
Order By (tenant_id,user_id, created_at)


insert into profiles values
('tiki', 'user1tiki','user1prime',['user1@gmail.com','0395669219'],['birthday','height'],[1995,165],['color','local','size'],['blue','HCM','M'],'2020-01-01 09:00:00','2020-01-01 09:00:00', 1),
('tiki', 'user2tiki','user2prime',['user2@gmail.com','0395669219'],['birthday','height'],[1995,166],['color','local','size'],['blue','HN','M'],'2020-01-01 10:00:00','2020-01-01 10:00:00', 1),
('tiki', 'user3tiki','user3prime',['user3@gmail.com','0395669219'],['birthday','height'],[1996,167],['color','local','size'],['blue','HCM','L'],'2020-01-01 08:00:00','2020-01-01 08:00:00', 1),
('tiki', 'user4tiki','user4prime',['user4@gmail.com','0395669219'],['birthday','height'],[1997,167],['color','local','size'],['blue','HCM','L'],'2020-01-01 08:00:00','2020-01-01 08:00:00', 1)

insert into profiles values
('shope', 'user1shope','user5prime',['user1@gmail.com','0395669219'],['birthday','height'],[1995,165],['color','local','size'],['blue','HCM','M'],'2020-01-01 10:00:00','2020-01-01 10:00:00', 1),
('shope', 'user2shope','user6prime',['user2@gmail.com','0395669219'],['birthday','height'],[1996,166],['color','local','size'],['blue','HN','M'],'2020-01-01 09:00:00','2020-01-01 09:00:00', 1)

insert into profiles values
('shope', 'user3shope','user5prime',['user2@gmail.com','0395669219'],['birthday','birthday'],[1997,1998],['color1','local','size1','size2'],['blue','HN','M','S'],'2020-01-01 09:00:00','2020-01-01 09:00:00', 1)




-- count distinct anonymous
select count(distinct anonymous_id) from profiles
select count(anonymous_id) from profiles
select count(*) from profiles

-- select test
select * from profiles where anonymous_id = 'user1prime'
select num_properties.keys, num_properties.vals from profiles where anonymous_id = 'user1prime'



-- count user birthday = 1995 local = HCM


select
    num_properties.vals as birthday,
    str_properties.vals as location,
    count() as num_user
from
    (
        select *
        from profiles
        array join num_properties
        where  num_properties.keys = 'birthday'
    )
array join str_properties
where str_properties.keys = 'local'
group by (birthday, location)

-- Array join lam cham

----------------------------------
select
    num_properties.vals[indexOf(num_properties.keys,'birthday')] as birthday,
    str_properties.vals[indexOf(str_properties.keys,'local')] as location,
    count() as num_user
from profiles
group by(birthday, location)





------------------------------------------------Materialized View---------------------------------------------------

create table profiles(
    anonymous_id String,
    num_properties Nested(
        keys String,
        vals Float32
    ),
    str_properties Nested(
        keys String,
        vals String
    ),
    created_at DateTime
)Engine = MergeTree()
Partition by toYYYYMM(created_at)
Order By (anonymous_id, created_at)

---------------------------------------------------

create table profiles_summing(
    day DateTime,
    profile_count AggregateFunction(count),
    max_height_state AggregateFunction(max, Float32),
    min_height_state AggregateFunction(min, Float32),
    avg_height_state AggregateFunction(avg, Float32)
)
Engine = SummingMergeTree()
Partition by tuple()
Order by (day)

----------------------------------------------------

create materialized view profiles_sum_mv
to profiles_summing
as select

        toStartOfDay(created_at) as day,
        countState() as profile_count,
        maxState(num_properties.vals[indexOf(num_properties.keys,'height')]) as max_height_state,
        minState(num_properties.vals[indexOf(num_properties.keys,'height')]) as min_height_state,
        avgState(num_properties.vals[indexOf(num_properties.keys,'height')]) as avg_height_state
    from profiles
    group by (day)
    order by (day)


----------------------------------------------------------------------
insert into profiles values
('user1prime',['height','weight'],[165,56],['location','birthday'],['HCM','1994'],'2020-01-01 00:00:00'),
('user2prime',['height','weight'],[162,52],['location','birthday'],['HN','1995'],'2020-01-01 00:01:00'),
('user3prime',['height','weight'],[170,64],['location','birthday'],['HCM','1996'],'2020-01-01 00:02:00'),
('user4prime',['height','weight'],[165,56],['location','birthday'],['HN','1997'],'2020-01-02 00:00:00'),
('user5prime',['height','weight'],[162,52],['location','birthday'],['HCM','1996'],'2020-01-02 00:01:00'),
('user6prime',['height','weight'],[170,64],['location','birthday'],['HCM','1996'],'2020-01-02 00:02:00'),

---------------------------------------------------- Thong ke theo ngay
select
    day,
    countMerge(profile_count) as profile_count,
    maxMerge(max_height_state) as max_height,
    minMerge(min_height_state) as min_height,
    avgMerge(avg_height_state) as avg_height
from profiles_summing
group by (day)

---------------------------------------------------- Thong ke theo thang
insert into profiles values
('user7prime',['height','weight'],[165,56],['location','birthday'],['HCM','1995'],'2020-02-01 00:00:00'),
('user8prime',['height','weight'],[162,52],['location','birthday'],['HCM','1996'],'2020-02-01 00:01:00'),
('user9prime',['height','weight'],[170,64],['location','birthday'],['HCM','1997'],'2020-02-01 00:02:00')

select
    toStartOfMonth(day) AS month,
    maxMerge(max_height_state) as max_height,
    minMerge(min_height_state) as min_height,
    avgMerge(avg_height_state) as avg_height
from profiles_sum
group by month

-------------------------------------------------------------------------------------------



------------------------------------------------------Other Example
insert into profiles values
('user7prime',['height','weight'],[165,56],['location','birthday'],['HCM','1995'],'2020-02-01 00:00:00'),
('user7prime',['height','weight'],[165,56],['location','birthday'],['HN','1996'],'2020-02-01 00:00:00'),
('user7prime',['height','weight'],[165,56],['location','birthday'],['HCM','1997'],'2020-02-01 00:00:00')


create table loc_birth_sum(
    day DateTime,
    location String,
    birthday String,
    count AggregateFunction(count)
)
Engine = SummingMergeTree()
Partition by tuple()
Order by (day)

create materialized view loc_birth_sum_mv
to loc_birth_sum
as
    select
        toStartOfDay(created_at) as day,
        str_properties.vals[indexOf(str_properties.keys, 'location')] as location,
        str_properties.vals[indexOf(str_properties.keys, 'birthday')] as birthday,
        countState() as count
    from profiles
    group by(day, location, birthday)



insert into profiles values
('user7prime',['height','weight'],[165,56],['location','birthday'],['HCM','1995'],'2020-02-01 00:00:00'),
('user7prime',['height','weight'],[165,56],['location','birthday'],['HN','1996'],'2020-02-01 00:00:00'),
('user7prime',['height','weight'],[165,56],['location','birthday'],['HCM','1997'],'2020-02-01 00:00:00')

---------------------------------------select per day
select
    day,
    location,
    birthday,
    countMerge(count) as count_per_day
from loc_birth_sum
group by(day,location, birthday)

---------------------------------------select per month

select
    toStartOfMonth(day),
    location,
    birthday,
    countMerge(count) as count_per_day
from loc_birth_sum
group by(day,location, birthday)

insert into profiles values
('user2prime',['height','weight'],[162,52],['location','birthday'],['HN','1995'],'2020-01-01 00:01:00')


----------------------------------------------------------------------------




-- Test insert multi profile

insert into profiles
select
      concat('a',toString(rand()%10)) as anonymous_id,
      ['height','weight'],
      [rand()%180,rand()%80] ,
      [] ,
      [] ,
    now() + number * 60 as created_at
from system.numbers
LIMIT 10000



-- Test nhieu aggregate column


    max_weight_state AggregateFunction(max, Float32),
    min_weight_state AggregateFunction(min, Float32),
    avg_weight_state AggregateFunction(avg, Float32)


















--------------------------------------------- MORE
drop table profiles;
drop table profiles_sum;
drop table profiles_sum_mv;
drop table loc_birth_sum;
drop table loc_birth_sum_mv;




truncate table profiles

alter table profiles modify column column_name column_type --
alter table profiles update anonymous_id = 'user4prime'  where user_id = 'user1shope' -- updated
alter table profiles update anonymous_id = 'user5prime'  where user_id = 'user2shope' -- updated

alter table profiles delete where user_id = 'user3shope'




















