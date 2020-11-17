SELECT t1.date_time AS date_time,
       toUnixTimestamp(date_time) as time_stamp,
       t2.clicks AS value
FROM
(
    SELECT
           count(campaign_id) AS clicks,
           toStartOfDay(FROM_UNIXTIME(at)) AS date_time
    FROM
    (
        SELECT
           count(),
            `campaign.vals`[indexOf(`campaign.keys`,'utm_campaign')] as utm_campaign,

        FROM events_campaign
        WHERE tenant_id = 1
          AND event_name = 'click'
          AND utm_campaign = 'c1'
          AND created_at >= '2020-01-01 00:00:00'
          AND created_at <= '2020-01-04 00:00:00'
        GROUP BY created_date
        ORDER BY created_date WITH FILL FROM toDateTime('2020-01-01 00:00:00') TO toDateTime('2020-01-04 00:00:01') STEP 1
    )
    GROUP BY date_time
)
;

select toDate('2020-01-01 00:01:00') as d, toTypeName(d);

---------------

WITH
    `campaign.vals`[indexOf(`campaign.keys`,'utm_campaign')] as utm_campaign
SELECT
    toStartOfHour(created_at) as time_stamp,
    count() as value
FROM events_campaign
WHERE tenant_id = 1
  AND event_name = 'click'
  AND utm_campaign = 'c1'
  AND created_at >= '2020-01-01 00:00:00'
  AND created_at <= '2020-01-04 00:00:00'
GROUP BY time_stamp
ORDER BY time_stamp WITH FILL FROM toDateTime('2020-01-01 00:00:00') TO toDateTime('2020-01-04 00:00:01') STEP 1*60*60
;

select event_name, created_at
from events_campaign
where event_name = 'click'
order by created_at;

--- test more group by

insert into events_campaign values
(randomPrintableASCII(3), 1, 'a2', 'click', 'engagement', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device'],['Male','HN','Phone'],[],[],[],[], '2020-01-01','2020-01-01 00:00:07')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a3', 'click', 'engagement', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device'],['Male','HN','Phone'],[],[],[],[], '2020-01-01','2020-01-01 00:00:12')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a4', 'click', 'engagement', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device'],['Male','HN','Phone'],[],[],[],[], '2020-01-02','2020-01-02 00:00:07')


insert into events_campaign values
(randomPrintableASCII(3), 1, 'a1', 'click', 'engagement', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device'],['Male','HN','Phone'],[],[],[],[], '2020-01-04','2020-01-04 00:00:07')



WITH
    `campaign.vals`[indexOf(`campaign.keys`,'utm_campaign')] AS utm_campaign
SELECT
    toStartOfHour(created_at) as time_stamp,
    COUNT() AS value,
    toUnixTimestamp(time_stamp)
FROM events_campaign
WHERE tenant_id = 1
  AND event_name = 'click'
  AND utm_campaign = 'c1'
  AND created_at >= '2020-01-01 00:00:00'
  AND created_at <= '2020-01-04 00:00:00'
GROUP BY time_stamp
ORDER BY time_stamp WITH FILL FROM toDateTime('2020-01-01 00:00:00') TO toDateTime('2020-01-04 00:00:01') STEP 24*60*60
;
select toUnixTimestamp();



select FROM_UNIXTIME(1577836800);



--------------------------------TEST CAMPAIGN REVENUE/ 2 LINE
select * from events limit 30;
select * from events_campaign;


SELECT
       tenant_id,
       utm_campaign as campaign_id,
       sumOrDefault(total_value) AS revenue,
       countOrDefault(DISTINCT anonymous_id) AS distinct_users,
       countOrDefault(anonymous_id) AS users
FROM
(
    SELECT
        `tenant_id`,
        anonymous_id,
       `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] AS total_value,
        `campaign.vals`[indexOf(`campaign.keys`, 'utm_campaign')] AS utm_campaign
    FROM events_campaign
    WHERE tenant_id = 1
        AND utm_campaign = 'c1'
        AND created_at >= '2020-01-01 00:00:00'
        AND created_at <= '2020-01-04 00:00:00'
)
GROUP BY tenant_id, utm_campaign;

select toUnixTimestamp('2020-01-01 00:00:00') as start,
       toUnixTimestamp('2020-01-02 00:00:00') as end
;




select * from events_campaign where event_name = 'click';


WITH
    `campaign.vals`[indexOf(`campaign.keys`,'utm_campaign')] AS utm_campaign
SELECT
    toStartOfHour(created_at) as time_stamp,
    COUNT() AS value
FROM events_campaign
WHERE tenant_id =1
  AND event_name = 'click'
  AND utm_campaign ='c1'
  AND created_at >= '2020-01-01 00:00:00'
  AND created_at <= '2020-01-02 00:00:01'
GROUP BY time_stamp
ORDER BY time_stamp WITH FILL FROM toDateTime('2020-01-01 00:00:00') TO toDateTime('2020-01-02 00:00:01') STEP 60*60


insert into events_campaign values
(randomPrintableASCII(3), 1, 'a11', 'click','engagement', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_3', 'google'], [],[],['gender','location_city','device'],['Male','HCM','PC'],[],[],[],[], '2020-01-01','2020-01-01 01:00:02')
;



WITH
    `campaign.vals`[indexOf(`campaign.keys`,'utm_campaign')] AS utm_campaign
SELECT
    toStartOfDay(created_at) as time_stamp,
    COUNT() AS value
FROM events_campaign
WHERE tenant_id = 1
  AND event_name = 'click'
  AND utm_campaign = 'c1'
  AND created_at >= '2020-01-01 00:00:00'
  AND created_at <= '2020-01-05 00:00:00'
GROUP BY time_stamp
ORDER BY time_stamp WITH FILL FROM toDateTime('2020-01-01 00:00:00') TO toDateTime('2020-01-05 00:00:00') STEP 86400
;

select * from events_campaign;

select toUnixTimestamp('2020-01-01 00:00:00') as start,
       toUnixTimestamp('2020-01-05 00:00:00') as end;


