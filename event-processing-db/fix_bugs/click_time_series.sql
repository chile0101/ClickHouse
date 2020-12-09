SELECT
    toStartOfDay(at) as time_stamp,
    count(DISTINCT (anonymous_id, utm_content, utm_source)) AS value
FROM events_campaign
WHERE tenant_id =1
  AND event_name = 'click'
  AND utm_campaign = 'CAMPAIGN dimension'
  AND toUnixTimestamp(at) >= 1606203885
  AND toUnixTimestamp(at) <= 1606549485
GROUP BY time_stamp
ORDER BY time_stamp WITH FILL FROM toStartOfDay(FROM_UNIXTIME(1606203885)) TO toStartOfDay(FROM_UNIXTIME(1606549485)) STEP 86400
;


select toStartOfDay(FROM_UNIXTIME(1606203885));


2020-11-26 15:16:17,741 INFO sqlalchemy.engine.base.Engine {'tenant_id': 1, 'event_name': 'click', 'utm_campaign': 'CAMPAIGN dimension', 'start_time': 1606203885, 'end_time': 1606549485}


SELECT
    toStartOfFifteenMinutes(at) as time_stamp,
    count(DISTINCT (anonymous_id, utm_content, utm_source)) AS value
FROM events_campaign
WHERE tenant_id =1
  AND event_name = 'click'
  AND utm_campaign = 'CAMPAIGN dimension'
  AND toUnixTimestamp(at) >= 1606203885
  AND toUnixTimestamp(at) <= 1606549485
GROUP BY time_stamp
ORDER BY time_stamp WITH FILL FROM toStartOfDay(FROM_UNIXTIME(1606203885)) TO toStartOfDay(FROM_UNIXTIME(1606549485)) STEP 86400
;
