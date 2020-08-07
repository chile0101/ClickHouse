--  Store complete data in MergeTree table, and use SummingMergeTree for aggregated data storing
-- Preparing reports

CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key


INSERT INTO summtt Values(1,1),(1,2),(2,1)


select key, sum(value)
from summtt
group by (key)



CREATE TABLE empty_summing (
    d Date,
    k UInt64, v Int8
) ENGINE=SummingMergeTree(d, k, 8192);

INSERT INTO empty_summing VALUES ('2015-01-01', 1, 10), ('2015-01-01', 1, -10);

OPTIMIZE TABLE empty_summing;
SELECT * FROM empty_summing;

INSERT INTO empty_summing VALUES ('2015-01-01', 1, 4),('2015-01-01', 2, -9),('2015-01-01', 3, -14);
INSERT INTO empty_summing VALUES ('2015-01-01', 1, -2),('2015-01-01', 1, -2),('2015-01-01', 3, 14);
INSERT INTO empty_summing VALUES ('2015-01-01', 1, 0),('2015-01-01', 3, 0);

OPTIMIZE TABLE empty_summing;
SELECT * FROM empty_summing;

DROP TABLE empty_summing;




---------------------------------------------------------------------------------------------





CREATE TABLE counter (
  when DateTime DEFAULT now(),
  device UInt32,
  value Float32,
  num_pros Nested(
    keys String,
    vals UInt32
  ),
  str_pros Nested(
    keys String,
    vals String
  )
) ENGINE=MergeTree
PARTITION BY toYYYYMM(when)
ORDER BY (device, when)

INSERT INTO counter values
    ('2017-01-01 00:00:00', 1,10, ['total_time'],[12],['brower','location'],['ff','HCM']),
    ('2017-01-01 00:00:00', 1,10, ['total_time'],[10],['brower','location'],['ch','HCM']),
    ('2017-01-02 00:00:00', 1,20, ['total_time'],[8],['brower','location'],['ee','HN']),
    ('2017-01-02 00:00:00', 1,30, ['total_time'],[16],['brower','location'],['ff','HCM']),
    ('2017-01-03 00:00:00', 1,30, ['total_time'],[11],['brower','location'],['ch','HCM']),

    ('2017-01-01 00:00:00', 2,10,['total_time'],[9],['brower','location'],['ee','HN']),
    ('2017-01-01 00:00:00', 2,10,['total_time'],[12],['brower','location'],['ff','HN']),
    ('2017-01-02 00:00:00', 2,20,['total_time'],[2],['brower','location'],['ch','HN']),
    ('2017-01-02 00:00:00', 2,30,['total_time'],[5],['brower','location'],['ch','HCM']),
    ('2017-01-03 00:00:00', 2,20,['total_time'],[6],['brower','location'],['ee','HCM'])


---------------------------------------------------
create table counter_daily(
    day DateTime,
    device UInt32,
    count UInt64,
    max_value_state AggregateFunction(max, Float32),
    min_value_state AggregateFunction(min, Float32),
    avg_value_state AggregateFunction(avg, Float32),
) Engine = SummingMergeTree()
PARTITION BY tuple()
ORDER BY (device,day)
------------------------------------------------------
create materialized view counter_daily_mv
to counter_daily
as  select
        toStartOfDay(when) as day,
        device,
        count(*) as count,
        maxState(value) AS max_value_state,
        minState(value) AS min_value_state,
        avgState(value) AS avg_value_state
    from counter
    group by device, day
    order by device, day

--------------------------------------
select
    device,
    sum(count) as count,
    maxMerge(max_value_state) as max,
    minMerge(min_value_state) as min,
    avgMerge(avg_value_state) as avg
from counter_daily
group by device
order by device

------------------------------

create table counter_daily(
    day DateTime,
    device UInt32,
    count UInt64,
    maxState(num_pros.keys[1]) as max_time_state

) Engine = SummingMergeTree()
PARTITION BY tuple()
ORDER BY (device,day)

------------------------------------
select indexOf(str_pros.keys, 'location') from counter

select
    when,
    device,
    str_pros.vals[indexOf(str_pros.keys, 'brower')] as brower,
    str_pros.vals[indexOf(str_pros.keys, 'location')] as location
from counter

-----------------------------------------------------------------

select
    when,
    device,
    str_pros.vals
from counter
array join str_pros
where str_pros.keys = 'brower' and str_pros.keys = 'location'


-


