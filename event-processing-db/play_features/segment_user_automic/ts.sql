SELECT time_stamps[idx] AS time_stamp,
       total_users[idx] AS value
FROM
(
    SELECT
        groupArray(time_stamp) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
        SELECT
            {to_start_of}(at) AS time_stamp,
            round(avg(length(users))) AS total_user
        FROM
        (
            SELECT DISTINCT *
            FROM segment_users
            WHERE (tenant_id = :tenant_id)
                AND (segment_id = :segment_id)
                AND (at <= :end_time)
        )
        GROUP BY
            tenant_id,
            segment_id,
            time_stamp
        ORDER BY time_stamp WITH FILL FROM toDateTime(:start_time) TO toDateTime(:end_time) STEP {fill_steps}
    )
)
ARRAY JOIN arrayEnumerate(time_stamps) as idx
WHERE time_stamp >= :start_time
  AND time_stamp <= :end_time;





SELECT DISTINCT *
FROM segment_users
WHERE (tenant_id = 1)
    AND (segment_id = :segment_id)
    AND (at <= :end_time);



  select * from segment_users limit 10;



