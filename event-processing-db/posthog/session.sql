select * from events;

-------------- test trim
with '{"Campaign Name": "Cypher Monday", "Campaign Source": "Instagram", "Platform": "ios", "Products": "jean", "Purchased": "yes", "Watch Video Count": 19, "Last Event": null, "attribution channel": [], "Like Count": 83, "Purchase Count": 7, "Ad Impression Count": 15, "Ad Conversion Count": 2, "Message Sent Count": 12, "Message Opened Count": 6, "Comment Count": 0, "Channel Subscribe Count": 205, "Clicked Email Count": 2, "Sign In Count": 9, "Create Playlist Count": 1, "Upload Video Count": 5, "Change Settings Count": 1, "Experiment Group": 8, "Dislike Count": 0, "App Version": "Ver0.0.1", "Sign Up Count": 5, "$event_origin": "prime", "at": 1606780800, "$lib": "Server-Side", "$lib_version": "1.1.3"}' as properties
select trim(BOTH '"' FROM JSONExtractRaw(properties, '$lib'))
from events;

with '{"Campaign Name": "Cypher Monday", "Campaign Source": "Instagram", "Platform": "ios", "Products": "jean", "Purchased": "yes", "Watch Video Count": 19, "Last Event": null, "attribution channel": [], "Like Count": 83, "Purchase Count": 7, "Ad Impression Count": 15, "Ad Conversion Count": 2, "Message Sent Count": 12, "Message Opened Count": 6, "Comment Count": 0, "Channel Subscribe Count": 205, "Clicked Email Count": 2, "Sign In Count": 9, "Create Playlist Count": 1, "Upload Video Count": 5, "Change Settings Count": 1, "Experiment Group": 8, "Dislike Count": 0, "App Version": "Ver0.0.1", "Sign Up Count": 5, "$event_origin": "prime", "at": 1606780800, "$lib": "Server-Side", "$lib_version": "1.1.3"}' as properties
select JSONExtractString(properties, '$lib')
;

----------------
select * from events where event =  '$feature_flag_called';
select distinct event from events;


--------------
SELECT
    SUM(total),
    day_start
FROM
(
SELECT toUInt16(0) AS total, toStartOfMonth(toDateTime('2020-12-24 00:00:00') - number * 2592000) as day_start from numbers(4)
 UNION ALL
    SELECT
        AVG(session_duration_seconds) as total,
        toStartOfMonth(timestamp) as day_start
    FROM
        (
            SELECT
                distinct_id,
                uuid,
                session_uuid,
                session_duration_seconds,
                timestamp,
                session_end_ts
            FROM
            (
                SELECT
                    distinct_id,
                    uuid,
                    if(is_new_session, uuid, NULL) AS session_uuid,
                    is_new_session,
                    is_end_session,
                    if(is_end_session AND is_new_session, 0, if(is_new_session AND (NOT is_end_session), dateDiff('second', toDateTime(timestamp), toDateTime(neighbor(timestamp, 1))), NULL)) AS session_duration_seconds,
                    timestamp,
                    if(is_end_session AND is_new_session, timestamp, if(is_new_session AND (NOT is_end_session), neighbor(timestamp, 1), NULL)) AS session_end_ts
            --     *
                FROM
                (
                    SELECT
                        distinct_id,
                        uuid,
                        timestamp,
                        neighbor(distinct_id, -1) AS start_possible_neighbor,
                        neighbor(timestamp, -1) AS start_possible_prev_ts,
                        if((start_possible_neighbor != distinct_id) OR (dateDiff('minute', toDateTime(start_possible_prev_ts), toDateTime(timestamp)) > 30), 1, 0) AS is_new_session,
                        neighbor(distinct_id, 1) AS end_possible_neighbor,
                        neighbor(timestamp, 1) AS end_possible_prev_ts,
                        if((end_possible_neighbor != distinct_id) OR (dateDiff('minute', toDateTime(timestamp), toDateTime(end_possible_prev_ts)) > 30), 1, 0) AS is_end_session
                    FROM
                    (
                        SELECT
                            uuid,
                            timestamp,
                            distinct_id
                        FROM events
                        WHERE
                            team_id = 1
                            AND event != '$feature_flag_called'
                            and timestamp >= '2020-09-25 00:00:00'
                            and timestamp <= '2020-12-24 23:59:59'
                            AND trim(BOTH '"' FROM JSONExtractRaw(properties, '$browser')) = 'Chrome' -- AND team_id = 1 -- TODO
                        GROUP BY
                            uuid,
                            timestamp,
                            distinct_id
                        ORDER BY
                            distinct_id ASC,
                            timestamp ASC
                    )
                )
                WHERE (is_new_session AND (NOT is_end_session)) OR (is_end_session AND (NOT is_new_session)) OR (is_end_session AND is_new_session)
            )
            WHERE is_new_session

        )
        GROUP BY
            toStartOfMonth(timestamp)
)
GROUP BY
    day_start
ORDER BY
    day_start
;

0,2020-09-01
0,2020-10-01
0,2020-11-01
2212.714285714286,2020-12-01
;




--------------------------------------- session id
show tables ;
SELECT
        avgOrDefault(arraySum(delta_arr)) AS avg_session_duration
FROM
(
      SELECT session_id,
             arrayDifference(time_arr) AS delta_arr
      FROM
      (
            SELECT session_id,
                   groupArray(toUnixTimestamp(at)) AS time_arr
            FROM
            (
                SELECT session_id, at
                FROM events
                WHERE tenant_id = :tenant_id AND anonymous_id = :anonymous_id
                ORDER BY at
            )
            GROUP BY session_id
      )
);
--- edit

SELECT sum(total),
       day_start
FROM
(
    SELECT toUInt16(0) AS total,
           toStartOfMonth(toDateTime('2020-12-24 00:00:00') - number * 2592000) AS day_start
    FROM numbers(4)
    UNION ALL
    SELECT
            avgOrDefault(arraySum(delta_arr)) AS total,
            day_start
    FROM
    (
        SELECT arraySum(delta_arr) as session_duration_seconds,
               timestamp
        FROM (
              SELECT session_id,
                     arrayDifference(time_arr) AS delta_arr,
                     timestamp
              FROM
              (
                    SELECT session_id,
                           groupArray(toUnixTimestamp(at)) AS time_arr,
                           min(at) as timestamp
                    FROM
                    (
                        SELECT session_id, at
                        FROM events
                        WHERE tenant_id = 1
                        and at >= '2020-09-25 00:00:00'
                        and at <= '2020-12-24 23:59:59'
                        ORDER BY anonymous_id, at
                    )
                    GROUP BY session_id
              )
        )
    )
    GROUP BY day_start
)
GROUP BY day_start
ORDER BY day_start
    ;

--------------------- FULL FILL
        SELECT arraySum(delta_arr) as session_duration_seconds,
               timestamp
        FROM (
              SELECT session_id,
                     arrayDifference(time_arr) AS delta_arr,
                     timestamp
              FROM
              (
                    SELECT session_id,
                           groupArray(toUnixTimestamp(timestamp)) AS time_arr,
                           min(timestamp) as timestamp
                    FROM
                    (
                        SELECT session_id, timestamp
                        FROM events
                        WHERE team_id = %(team_id)s
                            AND event != '$feature_flag_called'
                            AND session_id != NULL
                                {date_from}
                                {date_to}
                                {filters}
                        ORDER BY session_id, timestamp
                    )
                    GROUP BY session_id
              )
        )




SELECT
            avgOrDefault(arraySum(delta_arr)) AS total,
            day_start
    FROM
    (
          SELECT session_id,
                 arrayDifference(time_arr) AS delta_arr,
                 day_start
          FROM
          (
                SELECT session_id,
                       groupArray(toUnixTimestamp(timestamp)) AS time_arr,
                       toStartOfMonth(min(timestamp)) as day_start
                FROM
                (
                    SELECT session_id, timestamp
                    FROM events
                    WHERE team_id = %(team_id)s
                        AND event != '$feature_flag_called'
                        AND session_id != NULL
                            {date_from}
                            {date_to}
                            {filters}
                    ORDER BY session_id, timestamp
                )
                GROUP BY session_id
          )
    )
    GROUP BY day_start
;


---- origin
SELECT
    distinct_id,
    uuid,
    session_uuid,
    session_duration_seconds,
    timestamp,
    session_end_ts
FROM
(
    SELECT
        distinct_id,
        uuid,
        if(is_new_session, uuid, NULL) AS session_uuid,
        is_new_session,
        is_end_session,
        if(is_end_session AND is_new_session, 0, if(is_new_session AND (NOT is_end_session), dateDiff('second', toDateTime(timestamp), toDateTime(neighbor(timestamp, 1))), NULL)) AS session_duration_seconds,
        timestamp,
        if(is_end_session AND is_new_session, timestamp, if(is_new_session AND (NOT is_end_session), neighbor(timestamp, 1), NULL)) AS session_end_ts
    FROM
    (
        SELECT
            distinct_id,
            uuid,
            timestamp,
            neighbor(distinct_id, -1) AS start_possible_neighbor,
            neighbor(timestamp, -1) AS start_possible_prev_ts,
            if((start_possible_neighbor != distinct_id) OR (dateDiff('minute', toDateTime(start_possible_prev_ts), toDateTime(timestamp)) > 30), 1, 0) AS is_new_session,
            neighbor(distinct_id, 1) AS end_possible_neighbor,
            neighbor(timestamp, 1) AS end_possible_prev_ts,
            if((end_possible_neighbor != distinct_id) OR (dateDiff('minute', toDateTime(timestamp), toDateTime(end_possible_prev_ts)) > 30), 1, 0) AS is_end_session
        FROM
        (
            SELECT
                uuid,
                timestamp,
                distinct_id
            FROM events
            WHERE
                team_id = %(team_id)s
                AND event != '$feature_flag_called'
                {date_from}
                {date_to}
                {filters}
            GROUP BY
                uuid,
                timestamp,
                distinct_id
            ORDER BY
                distinct_id ASC,
                timestamp ASC
        )
    )
    WHERE (is_new_session AND (NOT is_end_session)) OR (is_end_session AND (NOT is_new_session)) OR (is_end_session AND is_new_session)
)
WHERE is_new_session
{sessions_limit}
;

---test
select toUnixTimestamp(now64());


-------------------------------- UPDATE SCHEMA EVENT
show create table events;
CREATE TABLE posthog.events
(
    `uuid` UUID,
    `event` String,
    `session_id` Nullable(String),
    `properties` String,
    `timestamp` DateTime64(6, 'UTC'),
    `team_id` Int64,
    `distinct_id` String,
    `elements_chain` String,
    `created_at` DateTime64(6, 'UTC'),
    `_timestamp` DateTime,
    `_offset` UInt64
)
ENGINE = ReplacingMergeTree(_timestamp)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, toDate(timestamp), distinct_id, event)
SAMPLE BY uuid
;








show tables ;
select count() from events;


SELECT arraySum(delta_arr) as session_duration_seconds,
       timestamp
FROM (
      SELECT session_id,
             arrayDifference(time_arr) AS delta_arr,
             timestamp
      FROM
      (
            SELECT session_id,
                   groupArray(toUnixTimestamp(date_time)) AS time_arr,
                   min(date_time) as timestamp
            FROM
            (
                SELECT trim(BOTH '"' FROM JSONExtractRaw(properties, '#sessionId')) as session_id,
                       toDateTime(timestamp) as date_time
                FROM events
                WHERE team_id = 1
                    AND event != '$feature_flag_called'
                    AND session_id not in ['']
                ORDER BY session_id, timestamp
            )
            GROUP BY session_id
      )
)




















