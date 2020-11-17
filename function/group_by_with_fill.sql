create table test(
    anonymous_id String,
    value Int32,
    date Date,
    date_time DateTime
)Engine = MergeTree()
order by (anonymous_id, date, date_time)

show tables;

select * from test;

insert into test values
('a1', 1, '2020-01-01', '2020-01-01 00:00:00'),
('a2', 2, '2020-01-02', '2020-01-02 00:00:05'),
('a5', 4, '2020-01-05', '2020-01-05 00:10:01')
;
insert into test values
('a1', 1, '2020-01-01', '2020-01-01 00:01:00');

select count(),
       date
from test
group by date
order by date WITH FILL FROM 1577836800 TO 1577923200 STEP 2;

select toUnixTimestamp('2020-01-02 00:00:00');

select FROM_UNIXTIME(1577923200);


SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5