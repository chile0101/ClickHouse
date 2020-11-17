create table dt
(
 `timestamp` DateTime64(3),
 `event_id` UInt8
)
ENGINE = TinyLog;

INSERT INTO dt Values (1546300800000, 1);

insert into dt values ('2019-01-01 00:00:00', 2);

insert into dt values (now64(), 4);

select * from dt;

SELECT * FROM dt
WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3);

select toUnixTimestamp(now64());

select now64();



