-- DATE
-- Stored in two bytes as the number of days since 1970-01-01 (unsigned)



-- DateTime([timezone])
--  [1970-01-01 00:00:00, 2105-12-31 23:59:59].

SELECT toDateTime(now(), 'Asia/Ho_Chi_Minh') AS column, toTypeName(column) AS x

select toUInt64(now())