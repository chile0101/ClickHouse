WITH
    countIf(event_name_type=='conversion') AS orders,
    countIf(DISTINCT anonymous_id, event_name =='click') AS clicks,
    orders/clicks as rate
SELECT utm_content,
       utm_source,
       location,
       toStartOfDay(at) AS time_stamp,
       if(rate == inf or rate > 1 , 1, rate)
FROM campaign_performance_data
WHERE tenant_id = 1
  AND utm_campaign = 'ARPU Increase Experiment'
GROUP BY utm_content, utm_source, location, time_stamp
;

--         countIf(DISTINCT anonymous_id, event_name =='click') as clicks,
--                (countIf(event_name_type=='conversion')/ countIf(DISTINCT anonymous_id, event_name =='click')) * 100 as value
-- 30% Discount,Facebook,Nam Định,2020-11-07 00:00:00,1,0,∞
-- 30% Discount,Facebook,Quy Nhơn,2020-11-25 00:00:00,1,0,∞
-- 30% Discount,Facebook,Nam Định,2020-11-23 00:00:00,1,0,∞
-- 30% Discount,Facebook,Mỹ Tho,2020-11-15 00:00:00,1,0,∞

SELECT utm_content,
       utm_source,
       location,
       time_stamp,
       orders / clicks as conversion_rate
FROM (
     SELECT coalesce(c1.utm_content, c2.utm_content)                        AS utm_content,
            coalesce(c1.utm_source, c2.utm_source)                          AS utm_source,
            if(c1.location != '', c1.location, c2.location)                 AS location,
            if(c1.time_stamp > c2.time_stamp, c1.time_stamp, c2.time_stamp) AS time_stamp,
            orders,
            if(clicks == 0, 1, clicks)                                         clicks
     FROM (
              SELECT utm_content,
                     utm_source,
                     location,
                     toStartOfDay(at) AS time_stamp,
                     count()          as orders
              FROM campaign_performance_data
              WHERE tenant_id = 1
                AND utm_campaign = 'ARPU Increase Experiment'
                AND event_name_type == 'conversion'
              GROUP BY utm_content, utm_source, location, time_stamp
              ) AS c1
              FULL JOIN
          (
              SELECT utm_content,
                     utm_source,
                     location,
                     toStartOfDay(at)             AS time_stamp,
                     count(DISTINCT anonymous_id) AS clicks
              FROM campaign_performance_data
              WHERE tenant_id = 1
                AND utm_campaign = 'ARPU Increase Experiment'
                AND event_name == 'click'
              GROUP BY utm_content, utm_source, location, time_stamp
              ) AS c2
          ON (c1.utm_content = c2.utm_content)
              AND (c1.utm_source = c2.utm_source)
              AND (c1.location = c2.location)
              AND (c1.time_stamp = c2.time_stamp)
)