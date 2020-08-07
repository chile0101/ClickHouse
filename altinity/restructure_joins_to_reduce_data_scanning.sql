create table user(
    user_id String,
    name String
)engine = MergeTree()
order by user_id

insert into user
select
concat('u', toString(number)),
randomPrintableASCII(6)
from system.numbers
limit 10000

create table order(
    order_id String,
    user_id String,
    value Int32
)engine = MergeTree()
order by order_id

insert into order
select
concat('o', toString(number)),
concat('u', toString(rand()%1000)),
rand()%100
from system.numbers
limit 10000

--- join 1
select name, user_id, sum, avg
from
(
    select
        user_id,
        sum(value) as sum,
        avg(value) as avg
    from order
    group by user_id
    having avg > 80
    order by user_id
) o inner join user on o.user_id = user.user_id

--0.026 sec
--0.021

-- join 2
select name, user_id, sum(value) as sum, avg(value) as avg
from user
join order
on user.user_id = order.user_id
group by user_id, name
having avg > 80
order by user_id
-- 0.010 sec
-- 0.017




-- truncate
truncate table user
truncate table order