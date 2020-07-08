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
                         ('002','2017-01-01','click','iphone7'),
                         ('001','2017-01-02','click','iphone7')


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
) ENGINE = MergeTree()
PARTITION BY (date)
ORDER BY (event_type, product_id, date)

insert into events_unique
    select date, group_id, client_id, event_type, product_id, uniqState(visitor_id) AS value 
    from events 
    group by date, group_id, client_id, event_type, product_id


-- solution 
SELECT uniqMerge(value) AS c 
  FROM events_unique 
 WHERE client_id = ‘aaaaaaaa’ 
   AND event_type = ‘click’ 
   AND product_id = ‘product1’ 
   AND date >= ‘2017–01–20’ 
   AND date < ‘2017–02–20’;




