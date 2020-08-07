--------------------------Neu insert gia tri ngoai pham vi cua kieu thi sao ?
create table t(
    x Int8,
    y UInt8,
    data String
)Engine = Log();

insert into t values (12, 156, 'chi') ;

insert into t values ( 128, 257, 'toilaai');  -- -128 │   1 │ le

insert into t values ( 129, 258, 'toilaai'); -- -127 │ 2 │ toilaai │


insert into t values ( 12, 256, '256'); --│   12 │ 0 │ 256     │


------------------------ DateTime
CREATE TABLE dt
(
    `timestamp` DateTime('Europe/Moscow'),
    `event_id` UInt8
)
ENGINE = TinyLog;

-- inserting datetime as an integer,
-- it is treated as Unix Timestamp (UTC). 1546300800 represents '2019-01-01 00:00:00' UTC
INSERT INTO dt values (1546300800, 1);
select dateDiff('second',toDateTime('1970-01-01 00:00:00'), toDateTime('2019-01-01 00:00:00')); --1546300800
select toUnixTimestamp('2019-01-01 00:00:00');

-- inserting string value as datetime, it is treated as being in column timezone.
-- '2019-01-01 00:00:00' will be treated as being in Europe/Moscow timezone and saved as 1546290000.
INSERT INTO dt values ('2019-01-01 00:00:00', 2);
select toUnixTimestamp('2019-01-01 00:00:00', 'Europe/Moscow'); --1546290000.



select * from dt;
-- luu y:
-- Run in Console : UTC
2019-01-01 00:00:00,1
2018-12-31 21:00:00,2
-- Run in clickhouse-client: Europe/Moscow
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘


-- Filtering
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Europe/Moscow')

--DateTime column values can be filtered using a string value in WHERE predicate.
-- It will be converted to DateTime automatically:
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'



-- Getting a time zone for a DateTime type column
SELECT toDateTime(now(), 'Europe/Moscow') AS column, toTypeName(column) AS x;

-- same, luu y chay tren console chi tra ve now(), mayby co van de.
SELECT toDateTime(now(), 'Asia/Ho_Chi_Minh');
SELECT toTimeZone(now(), 'Asia/Ho_Chi_Minh');



-- ham nay chi convert to UnixTimestamp chu ko validate input
select toUnixTimestamp('2019-02-30 00:00:00'); -- 1551485100
select toUnixTimestamp('2019-02-32 00:00:00'); -- 0