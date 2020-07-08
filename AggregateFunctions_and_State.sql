create table events(
    visitor_id String,
    date Date,
    event_type String,
    product_id String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (visitor_id, date)

insert into events values ('001','2017-01-01','click','iphone7'),
                         ('001','2017-01-01','click','iphone7'),
                         ('001','2017-01-01','click','iphone7'),
                         ('002','2017-01-02','click','iphone7'),
                         ('003','2017-01-02','click','iphone7'),
                         ('004','2017-01-02','click','iphone8')

                        


-- Problem
SELECT uniq(visitor_id) AS c 
  FROM events 
 WHERE event_type = 'click'
    AND product_id = 'iphone7'
   AND date >= '2017-01-01'
   AND date <= '2017-01-02'

-- 0.948 sec
------------------------------------------SOLUTION-------------------------------
create table events_unique(
    date Date,
    event_type String,
    product_id String,
    value AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (event_type, product_id, date)


insert into events_unique 
    select date, event_type, product_id, uniqState(visitor_id) as value 
    from events
    group by date, event_type, product_id


   --------------------or use MV

CREATE MATERIALIZED VIEW events_unique_mv
TO events_unique 
AS  SELECT 
        date,
        event_type,
        product_id,
        uniqState(visitor_id) AS value
    FROM events 
    GROUP BY date, event_type, product_id


-- solution 

SELECT uniqMerge(value)
FROM events_unique
WHERE event_type = 'click' 
AND product_id = 'iphone7'
AND date >= '2017-01-01'
AND date <= '2017-01-02'

INSERT INTO events values ('005','2017-01-01','click','iphone7')





