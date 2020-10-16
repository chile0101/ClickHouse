-- DATE
-- Stored in two bytes as the number of days since 1970-01-01 (unsigned)



-- DateTime([timezone])
--  [1970-01-01 00:00:00, 2105-12-31 23:59:59].

SELECT toDateTime(now(), 'Asia/Ho_Chi_Minh') AS column, toTypeName(column) AS x

select toUInt64(now())



---------------------------------- DATETIME  COMMUNICATE FRONT_END vs BACK END

UTC:                    10h

client - VN             17h -- UTC + 7

 show Asia/HCM on UI  <= front-end => convert to UTC => send to Back end => Save to DB

client - Europe/Moscow: 13h


select toDateTime(now(), 'Asia/Ho_Chi_Minh')


-- Example

SELECT
    toUnixTimestamp(t1.data_point) AS data_point,
    t1.data_point as data_point_time,
    t2.value AS value
FROM
(
    SELECT data_point
    FROM
    (
        SELECT toStartOfDay(now()) + ((number * 60) * 60) AS data_point
        FROM system.numbers
        LIMIT 24
    )
    WHERE data_point <= toDateTime('2020')
) AS t1
LEFT JOIN
(
    SELECT
        time_stamp,
        uniqMerge(value) AS value
    FROM event_total_user_by_source_ts
    WHERE (tenant_id = 1)
        AND (source_id = 'IOS-1gazgMAxIp60M6k9YE3HdghgRda')
        AND (time_stamp >= toStartOfDay(now()))
        AND (time_stamp < now())
    GROUP BY
        tenant_id,
        source_id,
        time_stamp
    ORDER BY
        tenant_id ASC,
        source_id ASC,
        time_stamp ASC
) AS t2 ON t1.data_point = t2.time_stamp
;


SELECT data_point
FROM
(
    SELECT toStartOfDay(toDateTime('2020-09-21 17:00:00')) + ((number * 60) * 60) AS data_point
    FROM system.numbers
    LIMIT 24
)
WHERE data_point <= toDateTime('2020-09-22 10:00:00')
    AND data_point >= toDateTime('2020-09-21 17:00:00')
;

select subtractHours(toDateTime('2020-09-22 00:00:00'), 7);


select addHours(toStartOfHour(now()), 1)