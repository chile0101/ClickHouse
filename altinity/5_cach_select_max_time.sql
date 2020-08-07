https://altinity.com/blog/2020/4/8/five-ways-to-handle-as-of-queries-in-clickhouse


CREATE TABLE downloads (
    when DateTime,
    userid UInt32,
    bytes Float32
) ENGINE=MergeTree
PARTITION BY toYYYYMM(when)
ORDER BY (userid, when)

insert into downloads values
(now(), 1, 12),
(now() + 1*60, 1, 24),
(now() + 2*60, 2 , 8),
(now(), 2, 14)

-- Bai toan: Lan cuoi ma moi userid download la khi nao ?
select * from downloads;
---------------------------------------- C1: Traditional
select userid, when, bytes
from downloads
join (
    select
        max(when) as time ,
        userid
    from downloads
    group by userid ) as d
on (downloads.userid = d.userid
        and downloads.when = d.time)


-----------------------------------------c2: IN
select *
from downloads
where (when, userid) IN
(
    select
        max(when) as time ,
        userid
    from downloads
    group by userid
    )

-----------------------------------------c3: Aggregate Function
select userid, max(when), argMax(bytes, when)
from downloads
group by userid;

------------------------------------------c5: ASOF JOIN



-----------------------------------------c4: LIMIT BY
--  slowest, butt it can do something special
-- One can use LIMIT 3 and return the 3 last measurements by time point
select *
from downloads
order by userid ASC, when DESC
LIMIT 1 BY userid;

