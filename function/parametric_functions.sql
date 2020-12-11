show tables;

create table test(
    time DateTime,
    number UInt16
)ENGINE = MergeTree
order by time;

insert into test values
(now(), 1), (now() + 1, 3), (now()+ 2, 4);


select sequenceMatch('(?1)(?2)')( time, number=1, number=2) from test;


select tenant_id,
       anonymous_id,
       event_name,
       at
from events;

select sequenceCount('(?1)(?2)')(at, event_name = 'view', event_name='checkout_completed')
from (
     select tenant_id,
       anonymous_id,
       event_name,
       at
from events
     where anonymous_id = 'DET9BOMWtFGk6Gar0e6jZ0r8Iak'
);


-- performed checkout completed greater than 1 within 30;

select tenant_id,
       anonymous_id,
       count()
from events
where event_name = 'view'
  and at >= subtractDays(now(), 30) and at <= now()
group by tenant_id, anonymous_id
having count() >= 3;


-- performed  checkout completed greater than 3 time


----------------------window funnel
select level,
       count() as c
from (
    select anonymous_id,
        windowFunnel(864000)(at, event_name='view', event_name='checkout_completed') as level
    -- select *
    from events
    where at>= '2020-11-01 00:00:00' and at <= '2020-12-04 00:00:00'
    group by anonymous_id
) group by level
order by level asc
;



select * from events;
select 10*86400;


select
    windowFunnel(864000)(at, event_name='identify', event_name='view')
-- select *
from events
where at >= '2020-12-01 00:00:00' and at <= '2020-12-04 00:00:00';

select * from events where anonymous_id ='DFOXyDNwDdISCqSkMX2d2kCK9Ca' order by at desc ;


------------------------------------------------RETENTION

CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);

SELECT * FROM retention_test;


select uid,
       retention(date='2020-01-01', date='2020-01-02', date='2020-01-03') as r
from retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
;



SELECT
    sum(r[1]) AS r1,
    sum(r[2]) AS r2,
    sum(r[3]) AS r3
FROM
(
    SELECT
        uid,
        retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
    FROM retention_test
    WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
    GROUP BY uid
)
;

