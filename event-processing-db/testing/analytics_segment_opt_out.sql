SELECT time_stamps[idx] AS time_stamp,
       total_users[idx] AS value
FROM
(
    SELECT
           groupArray(time_stamp) AS time_stamps,
           arrayCumSum(groupArray(total_user)) AS total_users
    FROM
    (
        SELECT  toStartOfHour(at) AS time_stamp,
                COUNT(DISTINCT user)    AS total_user
        FROM segment_user_final_v
        WHERE tenant_id = 1
          AND segment_id = '1kY4TW6829Gy0E0mei9uDMsrE3W'
          AND toUnixTimestamp(at) <= 1610942438
        GROUP BY tenant_id, segment_id, time_stamp
           ORDER BY time_stamp WITH FILL FROM toStartOfHour(toDateTime(1610254800)) TO toStartOfHour(toDateTime(1610942438)) STEP 3600
    )
)
ARRAY JOIN arrayEnumerate(time_stamps) AS idx
WHERE time_stamp >= 1610254800
 AND time_stamp <= 1610942438

;

select * from segment_user;
select * from segment_user_final_v;

select * from segment_user;

CONFIG_SOURCE=http://localhost:8500;CONFIG_NAMESPACE=eventify/dev




SELECT count() as count
FROM segment_user
WHERE tenant_id = 0 and segment_id = 'test_segment_opt_in'
;
select count() from segment_user;


