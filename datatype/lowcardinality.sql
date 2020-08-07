-- LowCardinality Data Type
--  most often used for strings
-- The magic can be applied to the existing data.
-- Reduce Storage Cost  -> increase speed

https://altinity.com/blog/2019/3/27/low-cardinality
https://altinity.com/blog/2020/5/20/reducing-clickhouse-storage-cost-with-the-low-cardinality-type-lessons-from-an-instana-engineer

CREATE TABLE downloads (
    when DateTime,
    userid UInt32,
    bytes Float32,
    hash1 String,
    hash2 LowCardinality(String)
) ENGINE=MergeTree
PARTITION BY toYYYYMM(when)
ORDER BY (userid, when);


INSERT INTO downloads
    SELECT
        now() + number*60 as when,
        rand()%1000,
        rand() %100000000,
        concat('data', toString(number)),
        concat('data', toString(number))
    FROM system.numbers
    LIMIT 10000;


-- The size on disk
select
    table,
    formatReadableSize(sum(bytes_on_disk))
from system.parts
where active = 1 and table = 'downloads'
group by table;

-----
select column, any(type),
       sum(column_data_compressed_bytes) compressed,
       sum(column_data_uncompressed_bytes) uncompressed,
       sum(rows)
from system.parts_columns
where table = 'downloads' and active = 1
group by column
order by column asc;
--hash,String,110.467.463,110.000.000,10.000.000

-- change type of Hash column to LowCardinality
alter table downloads modify column `hash` LowCardinality(String);
--hash,LowCardinality(String),118.072.947,130.044.847,10000000

-- Tam thoi chua thay hieu qua.















select count() from downloads; -- 0.001
select count(userid) from downloads; -- 0.017
select count(when) from downloads; -- 0.096


select
    toStartOfDay(when) as day,
    userid,
    count(),
    sum(bytes)
from downloads
group by day, userid
--  1.468 sec



