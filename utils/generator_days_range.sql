------------------------------------------------------V1----------------------------------------------
WITH
    60*60*24 AS delta,
    toDateTime('2020-10-01 00:00:00') AS start,
    toDateTime('2020-10-10 00:00:00') AS end,
    ceil((end - start) / delta) + 1 AS n
SELECT toStartOfDay(start) + number * delta AS date_time
FROM system.numbers
LIMIT n
;
-----------------------------------------------------V2 Unix-------------------------------------------
WITH
    24*60*60 AS delta,
    1601510400 AS start_unix,
    1601514000 AS end_unix,
    ceil((end_unix - start_unix) / delta) + 1 AS n
SELECT toStartOfDay(FROM_UNIXTIME(start_unix)) + number * delta AS date_time
FROM system.numbers
LIMIT n
;

select toUnixTimestamp(toDateTime('2020-10-01 00:00:00'));
SELECT toUnixTimestamp(toDateTime('2020-10-01 01:00:00'));


