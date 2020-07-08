CREATE DATABASE IF NOT EXISTS db_test


CREATE TABLE db_test.download (
    when DateTime,
    userid UInt32,
    bytes Float32
) ENGINE=MergeTree
PARTITION BY toYYYYMM(when)
ORDER BY (userid, when)


INSERT INTO db_test.download 
    SELECT 
        now() + number*60 as when,
        25, 
        rand() %100000000
    FROM system.numbers
    LIMIT 5000

SELECT COUNT() FROM db_test.download


SELECT 
    toStartOfDay(when) AS day,
    userid,
    count() AS downloads,
    sum(bytes) AS bytes
FROM db_test.download
GROUP BY userid, day 
ORDER BY userid, day 


CREATE MATERIALIZED VIEW download_daily_mv
ENGINE = SummingMergeTree



-------------------------------------------- Task-------------------------------------------------

SELECT * FROM event

CREATE TABLE event (
    userid String,
    event_name FixedString(1),
    date_time DateTime,
    value Float32
) ENGINE = MergeTree
PARTITION BY toYYYYMM(date_time)
ORDER BY (userid, date_time)


INSERT INTO event VALUES('0001','A','2020-07-06 08:00:00', rand()/100000000), 
                        ('0001','A','2020-07-06 08:30:00', rand()/100000000), 
                        ('0001','A','2020-07-06 09:00:00' ,rand()/100000000), 
                        ('0001','B','2020-07-06 08:00:00', rand()/100000000),
                        ('0001','B','2020-07-06 08:30:00', rand()/100000000),

                        ('0002','A','2020-07-06 10:00:00', rand()/100000000),
                        ('0002','A','2020-07-06 10:30:00', rand()/100000000),
                        ('0002','B','2020-07-06 11:00:00', rand()/100000000),
                        ('0002','B','2020-07-06 11:30:00', rand()/100000000),


                        ('0001','A','2020-07-07 08:00:00',rand()/100000000),
                        ('0001','A','2020-07-07 08:30:00',rand()/100000000),
                        ('0001','A','2020-07-07 09:00:00',rand()/100000000),
                        ('0001','B','2020-07-07 08:00:00',rand()/100000000),
                        ('0001','B','2020-07-07 08:30:00',rand()/100000000),

                        ('0002','A','2020-07-06 08:00:00',rand()/100000000),
                        ('0002','A','2020-07-06 08:30:00',rand()/100000000),
                        ('0002','B','2020-07-06 11:00:00',rand()/100000000),
                        ('0002','B','2020-07-06 11:30:00',rand()/100000000),

                        ('0001','A','2020-07-08 08:00:00',rand()/100000000), -- Lanmark
                        ('0001','A','2020-07-08 08:30:00',rand()/100000000), --
                        ('0002','A','2020-07-08 11:00:00',rand()/100000000), --
                        ('0003','A','2020-07-08 11:30:00',rand()/100000000)


------------------------ Manual 
SELECT 
    userid AS user,
    event_name AS event ,
    count() AS count,
    toStartOfDay(date_time)  AS date
FROM event
GROUP BY user, event, date
ORDER BY user, event, date 

----------------------- Use Materialized View

CREATE MATERIALIZED VIEW event_daily_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (userid, event_name, day)
POPULATE
AS  SELECT
        userid ,
        event_name  ,
        countState() as count_value_state,
        toStartOfDay(date_time) AS day
  
FROM event 
GROUP BY userid, event_name, day

---------------------- select directly from the materialized view.

SELECT  userid, event_name, countMerge(count_value_state) as count, day
FROM event_daily_mv
GROUP BY userid, event_name, day
ORDER BY userid, event_name, day

-------by month

SELECT 
    user,
    event,
    sum(count) as countByMonth,
    toStartOfMonth(day) as month 
FROM event_daily_mv 
GROUP BY user, event, month


-----------------------------------------------More Flexible Views
------Manual
SELECT 
    userid, 
    count(*) AS count,
    max(value) AS max,
    min(value) AS min,
    avg(value) AS avg
FROM event 
GROUP BY userid
ORDER BY userid

-------------- Create target table

CREATE TABLE event_daily (
    userid String,
    count_value_state AggregateFunction(count, Float32),
    max_value_state AggregateFunction(max, Float32),
    min_value_state AggregateFunction(min, Float32),
    avg_value_state AggregateFunction(avg, Float32)
)
ENGINE = SummingMergeTree()
PARTITION BY tuple()
ORDER BY (userid)

---- ---------Create Materialized View 

 CREATE MATERIALIZED VIEW event_daily_mv_2
 TO event_daily
 AS SELECT
    userid,
    countState(value) as count_value_state,
    maxState(value) as max_value_state,
    minState(value) as min_value_state,
    avgState(value) as avg_value_state
FROM event 
GROUP BY userid 
ORDER BY userid

--- Start at here / let's insert new data 

INSERT INTO event VALUES('0001','A','2020-07-08 08:00:00', rand()/100000000),
                        ('0001','B','2020-07-09 08:30:00', rand()/100000000),

                        ('0001','A','2020-07-10 08:00:00', rand()/100000000),
                        ('0002','A','2020-07-10 08:30:00', rand()/100000000)


-------- SELECT FROM VIEW

SELECT
  userid,
  countMerge(count_value_state) AS count,

  maxMerge(max_value_state) AS max,
  minMerge(min_value_state) AS min,
  avgMerge(avg_value_state) AS avg
FROM event_daily
GROUP BY userid
ORDER BY userid ASC



---------------------------------------------------try aggregate function vs POPULATE

 CREATE MATERIALIZED VIEW event_daily_mv_3
 ENGINE = SummingMergeTree
 ORDER BY (userid, day)
 POPULATE
 AS SELECT
    userid,
    toStartOfDay(date_time) as day,
    countState() as count_value_state,
    sumState(value) as sum_value_state,
    maxState(value) as max_value_state,
    minState(value) as min_value_state,
    avgState(value) as avg_value_state
FROM event 
GROUP BY userid, day 


-- WHERE date_time >= toDate('2020-07-08 08:00:00')


SELECT
  userid,
  day,
  sum(count) AS count,
  maxMerge(max_value_state) AS max,
  minMerge(min_value_state) AS min,
  avgMerge(avg_value_state) AS avg
FROM event_daily_mv_3
GROUP BY userid,day
ORDER BY userid ASC

---------------------It's OK -> Why should not use POPULATE : https://clickhouse.tech/docs/en/sql-reference/statements/create/#create-view



[1] -- insert-->  Raw table [event] --trigger + populate--> MV [event_daily_mv] <-- select


[2] -- insert-->  Raw table [event] --trigger --> MV [event_daily_mv] --to-->  AggregateTable  <-- select

-- 
-- thay MV schema (add column, change type)





---- OPTION [2]---- TO option views are easier to change 

CREATE TABLE event_daily (
    userid String,
    day DateTime,
    count UInt32,
    max_value_state AggregateFunction(max, Float32),
    min_value_state AggregateFunction(min, Float32)
   
)
ENGINE = SummingMergeTree()
PARTITION BY tuple()
ORDER BY (userid, day)


CREATE MATERIALIZED VIEW event_daily_mv_2
TO event_daily
AS SELECT
    userid,
    toStartOfDay(date_time) as day,
    count(*) as count,
    maxState(value) as max_value_state,
    minState(value) as min_value_state
FROM event 
GROUP BY userid, day 
ORDER BY userid, day


SELECT
  userid,
  day,
  sum(count) AS count,
  maxMerge(max_value_state) AS max,
  minMerge(min_value_state) AS min
FROM event_daily
GROUP BY userid,day
ORDER BY userid ASC

 -- drop view
DROP TABLE event_daily_mv_2

-- update target table
ALTER TABLE event_daily ADD COLUMN "avg_value_state" AggregateFunction(avg, Float32)

-- recreate view
CREATE MATERIALIZED VIEW event_daily_mv_2
TO event_daily
AS SELECT
    userid,
    toStartOfDay(date_time) as day,
    count(*) as count,
    maxState(value) as max_value_state,
    minState(value) as min_value_state,
    avgState(value) as avg_value_state
FROM event 
GROUP BY userid, day 
ORDER BY userid, day




------ OPTION [1] 
        
-- create view
 CREATE MATERIALIZED VIEW event_daily_mv_3
 ENGINE = SummingMergeTree
 ORDER BY (userid, day)
 POPULATE
 AS SELECT
    userid,
    toStartOfDay(date_time) as day,
    count() as count,
    maxState(value) as max_value_state,
    minState(value) as min_value_state
FROM event 
GROUP BY userid, day 


SELECT
  userid,
  day,
  sum(count) AS count,
  maxMerge(max_value_state) AS max,
  minMerge(min_value_state) AS min
FROM event_daily_mv_3
GROUP BY userid,day
ORDER BY userid ASC

-- detach view
DETACH TABLE event_daily_mv_3

-- update table
ALTER TABLE `.inner.event_daily_mv_3` ADD COLUMN "avg_value_state" AggregateFunction(avg, Float32)

-- attach view
ATTACH MATERIALIZED VIEW  event_daily_mv_3 
ENGINE = SummingMergeTree()
ORDER BY (userid, day)
 AS SELECT
    userid,
    toStartOfDay(date_time) as day,
    count() as count,
    maxState(value) as max_value_state,
    minState(value) as min_value_state,
    avgState(value) as avg_value_state
FROM event 
GROUP BY userid, day 