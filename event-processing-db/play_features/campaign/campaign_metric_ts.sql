select *
from events_campaign;



----------Date Time range
WITH
    24*60*60 AS delta,
    1601510400 AS start_unix,
    1601514000 AS end_unix,
    ceil((end_unix - start_unix) / delta) + 1 AS n
SELECT toStartOfDay(FROM_UNIXTIME(start_unix)) + number * delta AS date_time
FROM system.numbers
LIMIT n
;


------------- base
select toUnixTimestamp(toDateTime('2020-01-01 00:00:00')); -- 1577836800
select toUnixTimestamp(toDateTime('2020-01-03 00:00:55')); -- 1578009655



-----------Base
-- filter: gender, location, device.
SELECT DISTINCT *
FROM
(
    SELECT  tenant_id,
            anonymous_id,
            event_name,
            `str_properties.vals`[indexOf(`str_properties.keys`, 'utm_campaign')] AS utm_campaign,
            `str_properties.vals`[indexOf(`str_properties.keys`, 'utm_content')]  AS utm_content,
            `str_properties.vals`[indexOf(`str_properties.keys`, 'utm_source')]   AS utm_source,
            `str_properties.vals`[indexOf(`str_properties.keys`, 'gender')]       AS filter
    FROM events_campaign
    WHERE tenant_id = 1
        AND utm_campaign = 'campaign1'
        AND at >= FROM_UNIXTIME(1577836800)
        AND at <= FROM_UNIXTIME(1578009655)
)
;

--------new revenue
SELECT  tenant_id,
        anonymous_id,
        event_name,
        `str_properties.vals`[indexOf(`str_properties.keys`, 'utm_campaign')] AS utm_campaign,
        `str_properties.vals`[indexOf(`str_properties.keys`, 'utm_content')]  AS utm_content,
        `str_properties.vals`[indexOf(`str_properties.keys`, 'utm_source')]   AS utm_source,
        `str_properties.vals`[indexOf(`str_properties.keys`, 'gender')]       AS filter,
       sum(`num_properties.vals`[indexOf(`str_properties.keys`, 'total_value')])  as net_revenue
FROM events_campaign
WHERE tenant_id = 1
    AND utm_campaign = 'campaign1'
    AND at >= FROM_UNIXTIME(1577836800)
    AND at <= FROM_UNIXTIME(1578009655)
GROUP BY tenant_id, anonymous_id, event_name, utm_campaign, utm_content, utm_source, utm_source,









-- net_revenue: OK
-- num_of_customer: OK
-- avg_revenue_per_user: OK
-- total_order: OK
-- avg_value_per_order
--



------------------------------------UPDATED
select * from events_campaigns;

select tenant_id,
       anonymous_id,
       event_name,
       utm_campaign,
       utm_content,
       utm_source,
       gender,
       location_city,
       device,
       total_value,
       at ,
       toUnixTimestamp(at)
from events_campaigns
    WHERE tenant_id = 1
        AND utm_campaign = 'campaign1'
        AND at >= FROM_UNIXTIME(1577836800)
        AND at <= FROM_UNIXTIME(1578009655)
;








SELECT tenant_id,
       anonymous_id,
       event_name,
       utm_campaign,
       utm_content,
       utm_source,
       gender,
       location_city,
       device,
       total_value,
      toStartOfDay(at) AS date_time
FROM events_campaigns
    WHERE tenant_id = 1
        AND utm_campaign = 'c1'
        AND at >= FROM_UNIXTIME(1577836800)
        AND at <= FROM_UNIXTIME(1578009655)
;


-------------------------------BACKED CH FORMAT
select * from events_campaign;

drop table events_campaigns;



SELECT
    tenant_id,
    anonymous_id,
    event_name,
    `campaign.vals`[indexOf(`campaign.keys`, 'utm_campaign')] AS utm_campaign,
    `campaign.vals`[indexOf(`campaign.keys`, 'utm_content')] AS utm_content,
    `campaign.vals`[indexOf(`campaign.keys`, 'utm_source')] AS utm_source,
    `str_properties.vals`[indexOf(`str_properties.keys`, 'gender')] AS gender,
    `str_properties.vals`[indexOf(`str_properties.keys`, 'location_city')] AS location_city,
    `str_properties.vals`[indexOf(`str_properties.keys`, 'device')] AS device,
    `num_properties.vals`[indexOf(`num_properties.keys`, 'total_value')] AS total_value
FROM events_campaign
WHERE tenant_id = 1
  AND utm_campaign = 'c1'
  AND created_at >= FROM_UNIXTIME(1577836800)
  AND created_at <= FROM_UNIXTIME(1578009655)
;

SELECT
    tenant_id,
    anonymous_id,
    event_name,
    `campaign.vals`[indexOf(`campaign.keys`, 'utm_campaign')] AS utm_campaign,
    `campaign.vals`[indexOf(`campaign.keys`, 'utm_content')] AS utm_content,
    `campaign.vals`[indexOf(`campaign.keys`, 'utm_source')] AS utm_source,
    `str_properties.vals`[indexOf(`str_properties.keys`, 'gender')] AS gender,
    `str_properties.vals`[indexOf(`str_properties.keys`, 'location_city')] AS location_city,
    `str_properties.vals`[indexOf(`str_properties.keys`, 'device')] AS device,
    `str_properties.vals`[indexOf(`str_properties.keys`, 'goal_name')] AS goal_name,
    `num_properties.vals`[indexOf(`num_properties.keys`, 'total_value')] AS total_value,
    toUnixTimestamp(toStartOfDay(created_at)) AS time_stamp
FROM events_campaign
WHERE tenant_id = 1
  AND utm_campaign = 'c1'
  AND goal_name = 'g1'
  AND created_at >= FROM_UNIXTIME(1577836800)
  AND created_at <= FROM_UNIXTIME(1578009655);