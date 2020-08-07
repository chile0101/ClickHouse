

-- Thong ke 1 user download may lan trong 1 ngay, tong bytes, max, min, avg byte la bao nhieu.


CREATE TABLE downloads (
    when DateTime,
    userid UInt32,
    bytes Float32
) ENGINE=MergeTree
PARTITION BY toYYYYMM(when)
ORDER BY (userid, when)


CREATE TABLE download_daily(
    day DateTime,
    userid UInt32,
    count UInt64,
    sum_value_state AggregateFunction(sum, Float32),
    max_value_state AggregateFunction(max, Float32),
    min_value_state AggregateFunction(min, Float32),
    avg_value_state AggregateFunction(avg, Float32)

)ENGINE = SummingMergeTree()
PARTITION BY tuple()
ORDER BY (userid, day)


CREATE MATERIALIZED VIEW download_daily_mv
TO download_daily
AS SELECT
        toStartOfDay(when) as day,
        userid,
        count() as count,
        sumState(bytes) as sum_value_state,
        maxState(bytes) as max_value_state,
        minState(bytes) as min_value_state,
        avgState(bytes) as avg_value_state
    FROM downloads
    GROUP BY (userid,day)
    ORDER BY (userid,day)


insert into downloads values
(now() + 1* 60 , 1, 1),
(now() + 2* 60 , 1, 2),
(now() + 3* 60 , 1, 3),
(now() + 1* 60 , 2, 4),
(now() + 2* 60 , 2, 5),
(now() + 3* 60 , 2, 6),


insert into downloads
    select
        now() + number * 60 as when,
        rand()%5,
        rand()%100
    from system.numbers
    limit 100000000


------------------- aggregate function
select
    day,
    userid,
    sum(count),
    sumMerge(sum_value_state) as sum_bytes,
    maxMerge(max_value_state) as max_bytes,
    minMerge(min_value_state) as min_bytes,
    avgMerge(avg_value_state) as avg_bytes
from download_daily
group by (userid, day)

-- 1000000: 3475 rows in set. Elapsed: 0.022 sec. Processed 3.48 thousand rows, 314.26 KB (159.94 thousand rows/s., 14.46 MB/s.)
-- 100000000 : 0.287 sec
select
    toStartOfMonth(day) as month,
    userid,
    sum(count),
    sumMerge(sum_value_state) as sum_bytes,
    maxMerge(max_value_state) as max_bytes,
    minMerge(min_value_state) as min_bytes,
    avgMerge(avg_value_state) as avg_bytes
from download_daily
group by (userid, month)
-- 1000000: 0.014
-- 100000000:  0.109 sec
------------------ manual

SELECT
        toStartOfDay(when) as day,
        userid,
        count() as count,
        sum(bytes) as sum_value_state,
        max(bytes) as max_value_state,
        min(bytes) as min_value_state,
        avg(bytes) as avg_value_state
    FROM downloads
    GROUP BY (userid,day)
    ORDER BY (userid,day)
-- 1000000: 3475 rows in set. Elapsed: 0.074 sec. Processed 1.00 million rows, 12.00 MB (13.44 million rows/s., 161.32 MB/s.)
-- 100000000 :  2.018 sec
SELECT
        toStartOfMonth(when) as month,
        userid,
        count() as count,
        sum(bytes) as sum_value_state,
        max(bytes) as max_value_state,
        min(bytes) as min_value_state,
        avg(bytes) as avg_value_state
    FROM downloads
    GROUP BY (userid,month)
    ORDER BY (userid,month)
-- 1000000: 0.058
-- 100000000: 1.812 sec








-- TODO: try toStartOfFiveMinute
















String -> count, uniq
Num -> count, max, min, avg, sum, uniq