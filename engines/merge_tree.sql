-- Test performance of index.
create table if not exists alerts(
    alert_id String,
    alert_id_1 String,
    data String,
    created_at DateTime
)Engine = MergeTree()
partition by toYYYYMM(created_at)
order by (alert_id, created_at)


insert into alerts
select
    concat('a',toString(number)),
    concat('a',toString(number)),
    randomPrintableASCII(10),
    toDateTime('2020-01-01 00:00:00') + rand(2)%(3600*24*30)
from system.numbers
limit 1000000

select
    toStartOfDay(created_at) as day, count()
from alerts
where day = '2020-01-10 00:00:00'
group by day
--  Elapsed: 0.032 sec. Processed 1.00 million rows,


select
     count()
from alerts
where created_at < toDateTime('2020-01-10 00:00:00')
