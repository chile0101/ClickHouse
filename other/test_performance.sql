create table users(
    user_id String,
    gender UInt8
)Engine = MergeTree()
Order by (user_id)

insert into users
    select number%10, rand()%100 + 1
    from system.numbers
    limit 10000000


select
    count(gender),
    avg(gender) as avg,
    max(gender),
    min(gender)
from users
group by user_id
order by avg
limit 3

-- order by 0.2
-- limit 0.188, 0.136

-- from > where > group by > having > distinct > select > order by > limit

--===> IT OK to use LIMIT to estimate PERFORMACE