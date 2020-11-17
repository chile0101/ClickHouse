--Special functions for time series
- runningDifference -> Từ value -> Sự biến thiên
- runningAccumulate -> Từ sự biến thiên -> value (Acucumulate : tich luy)
- neighbor

-- Note:
-- A window function perform a calculation across a set of table rows that are somehow related to the current row.
-- giống như là aggregate function (SUM, AVG). Điểm khác biệt chính là khi dùng window function
-- các row sẽ không bị gộp lại thành một như khi dùng aggregate function.


-- Accumulate
SELECT k,
       runningAccumulate(sum_k) AS res
FROM (
      SELECT number as k,
             sumState(k) AS sum_k
      FROM numbers(10)
      GROUP BY k
      ORDER BY k
     );

-- Neghbor
SELECT number,
       neighbor(number, 2)
FROM system.numbers LIMIT 10;



-- sumMap
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt16,
        requests UInt64
    ),
    statusMapTuple Tuple(Array(Int32), Array(Int32))
) ENGINE = Log;


INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));

SELECT
    timeslot,
    sumMap(statusMap.status, statusMap.requests),
    sumMap(statusMapTuple)
FROM sum_map
GROUP BY timeslot
;


WITH
    `str_properties.vals`[indexOf(`str_properties.keys`, 'utm_campaign')] AS campaign_id
SELECT
    `num_properties.vals`[indexOf(`num_properties.keys`, 'total_value')] AS total_value,
    at
FROM events_campaign
WHERE tenant_id = 1
  AND campaign_id = 'campaign2'
  AND at >= FROM_UNIXTIME(1577836800)
  AND at <=FROM_UNIXTIME(1578009655)
ORDER BY at
;
-- timeSeriesGroupSum
CREATE TABLE time_series(
    uid       UInt64,
    timestamp Int64,
    value     Float64
) ENGINE = Memory;


INSERT INTO time_series VALUES
    (1,2,0.2),(1,7,0.7),(1,12,1.2),(1,17,1.7),(1,25,2.5),
    (2,3,0.6),(2,8,1.6),(2,12,2.4),(2,18,3.6),(2,24,4.8);

select * from time_series;

SELECT timeSeriesGroupSum(uid, timestamp, value)
FROM (
    SELECT * FROM time_series order by timestamp ASC
);

[(2,0.2),(3,0.8999999999999999),(7,2.0999999999999996),(8,2.4),(12,3.5999999999999996),(17,5.1000000000000005),(18,5.4),(24,7.199999999999999),(25,2.5)]
[(2,0.2),(3,0.9),(7,2.1),(8,2.4),(12,3.6),(17,5.1),(18,5.4),(24,7.2),(25,2.5)]


1,2,0.2
2,3,0.6
1,7,0.7
2,8,1.6
1,12,1.2
2,12,2.4
1,17,1.7
2,18,3.6
2,24,4.8
1,25,2.5













-- select toUnixTimestamp(t1.time_stamp),
--        t1.time_stamp,
--        t2.value
-- from
-- (
--     with
--         15 as delta,
--         1600707600 as start_unix,
--         1600793999 as end_unix,
--         FROM_UNIXTIME(start_unix) as start,
--         FROM_UNIXTIME(end_unix) as end,
--         ceil((end_unix - start_unix) / (60 * delta)) as n
--     select addMinutes(toStartOfFifteenMinutes(start), number * delta) as time_stamp
--     from system.numbers
--     limit n
-- ) as t1
-- left join
--     (
--         with
--             FROM_UNIXTIME(1600707600) as start,
--             FROM_UNIXTIME(1600793999) as end
--         SELECT time_stamp,
--             uniqMerge(value) AS value
--      FROM event_total_user_by_source_ts
--      WHERE (tenant_id = 1)
--        AND (source_id = 'Google Ads')
--        AND (time_stamp >= start)
--        AND (time_stamp <= end)
--      GROUP BY tenant_id,
--               source_id,
--               time_stamp
--     ) as t2
-- on t1.time_stamp = t2.time_stamp
-- ;
--
-- select *
-- from (
--       select groupArray(time_stamp)                                                   as ts,
--              arrayEnumerate(ts)                                                       as ts_row,
--              min(time_stamp)                                                          as ts_min,
--              arrayMap((x, y) -> (x - y), ts, arrayPushFront(arrayPopBack(ts), ts[1])) as ts_diff
--       from (
--             select time_stamp,
--                    uniqMerge(value) as value
--             from event_total_user_by_source_ts
--             group by tenant_id, source_id, time_stamp
--             order by time_stamp
--                )
-- ) array join ts as time_stamp, ts_diff as duration, ts_row as sequence
-- ;
--
-- with [5,7,9,12,16] as ts
-- select  ts,
--        arrayPushFront(arrayPopBack(ts), ts[1]) as ts_shift,
--        arrayMap((x,y) -> (x-y), ts, ts_shift )
--
--            ;
--
-- show create table event_total_user_by_source_ts;
--
-- show create table event_total_user_by_source_ts_mv;
--
-- select arrayZip(groupArray(time_stamp), groupArray(value))
-- from (
--       select time_stamp,
--              uniqMerge(value) as value
--       from event_total_user_by_source_ts
--       group by tenant_id, source_id, time_stamp
--       order by time_stamp
-- )
-- ;




