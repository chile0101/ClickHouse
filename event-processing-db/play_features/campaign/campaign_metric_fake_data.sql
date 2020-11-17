truncate table events_campaigns;

---------------
insert into events_campaigns values
(randomPrintableASCII(5),1, 'a1', 'click', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source1', 'Male', 'HCM', 'Mobile', NULL, NULL, NULL, '2020-01-01 00:00:00');

insert into events_campaigns values
(randomPrintableASCII(5),1, 'a1', 'reach_goal', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source1', 'Male', 'HCM', 'Mobile', 'g1', NULL, NULL, '2020-01-01 00:00:30');

insert into events_campaigns values
(randomPrintableASCII(5),1, 'a1', 'conversion', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source1', 'Male', 'HCM', 'Mobile', NULL, 2, 'VND', '2020-01-01 00:00:45');

---------------
insert into events_campaigns values
(randomPrintableASCII(5),1, 'a2', 'click', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source1', 'Male', 'HCM', 'Mobile', NULL, NULL, NULL, '2020-01-01 00:00:00');

-- insert into events_campaigns values
-- (randomPrintableASCII(5),1, 'a2', 'reach_goal', randomPrintableASCII(3), randomPrintableASCII(3),
--  'c1', 'content1', 'source1', 'Male', 'HCM', 'Mobile', NULL, NULL, NULL, '2020-01-01 00:00:30');
--
-- insert into events_campaigns values
-- (randomPrintableASCII(5),1, 'a2', 'conversion', randomPrintableASCII(3), randomPrintableASCII(3),
--  'c1', 'content1', 'source1', 'Male', 'HCM', 'Mobile', NULL, NULL, NULL, '2020-01-01 00:00:45');


---------------
insert into events_campaigns values
(randomPrintableASCII(5),1, 'a3', 'click', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source1', 'Male', 'HCM', 'PC', NULL, NULL, NULL, '2020-01-01 01:00:00');

insert into events_campaigns values
(randomPrintableASCII(5),1, 'a3', 'reach_goal', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source1', 'Male', 'HCM', 'PC', 'g1', NULL, NULL, '2020-01-01 01:00:30');

insert into events_campaigns values
(randomPrintableASCII(5),1, 'a3', 'conversion', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source1', 'Male', 'HCM', 'PC', NULL, 5, 'VND', '2020-01-01 01:00:45');
;

--------------
insert into events_campaigns values
(randomPrintableASCII(5),1, 'a4', 'click', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source1', 'Male', 'HN', 'Mobile', NULL, NULL, NULL, '2020-01-01 02:00:00');

-- insert into events_campaigns values
-- (randomPrintableASCII(5),1, 'a4', 'reach_goal', randomPrintableASCII(3), randomPrintableASCII(3),
--  'c1', 'content1', 'source1', 'Male', 'HN', 'Mobile', NULL, NULL, NULL, '2020-01-01 02:00:30');
--
-- insert into events_campaigns values
-- (randomPrintableASCII(5),1, 'a4', 'conversion', randomPrintableASCII(3), randomPrintableASCII(3),
--  'c1', 'content1', 'source1', 'Male', 'HN', 'Mobile', NULL, NULL, NULL, '2020-01-01 02:00:45');

--------------
insert into events_campaigns values
(randomPrintableASCII(5),1, 'a5', 'click', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source1', 'Female', 'HCM', 'Mobile', NULL, NULL, NULL, '2020-01-01 03:00:00');

insert into events_campaigns values
(randomPrintableASCII(5),1, 'a5', 'reach_goal', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source1', 'Female', 'HCM', 'Mobile', 'g2', NULL, NULL, '2020-01-01 03:00:30');

insert into events_campaigns values
(randomPrintableASCII(5),1, 'a5', 'conversion', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source1', 'Female', 'HCM', 'Mobile', NULL, 1, 'VND', '2020-01-01 03:00:45');

--------------
insert into events_campaigns values
(randomPrintableASCII(5),1, 'a6', 'click', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source2', 'Female', 'HCM', 'Mobile', NULL, NULL, NULL, '2020-01-01 04:05:00');

---------------
insert into events_campaigns values
(randomPrintableASCII(5),1, 'a5', 'conversion', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source1', 'Female', 'HCM', 'Mobile', NULL, 2, 'VND', '2020-01-01 05:00:45');





--------------------------------------------------
truncate table events_campaigns;


select *
from events_campaign
where event_name = 'conversion';



show create table events_campaigns;
CREATE TABLE eventify.events_campaigns
(
    `id` String,
    `tenant_id` UInt16,
    `anonymous_id` String,
    `event_name` String,
    `session_id` String,
    `scope` String,
    `utm_campaign` String,
    `utm_content` String,
    `utm_source` String,
    `gender` String,
    `location_city` String,
    `device` String,
    `goal_name` Nullable(String),
    `total_value` Nullable(String),
    `currency` Nullable(String),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, anonymous_id, utm_campaign, event_name, at)
SETTINGS index_granularity = 8192
;


select * from events_campaigns;



SELECT tenant_id,
       anonymous_id,
       event_name,
       event_name AS event_type,
       utm_campaign,
       utm_content,
       utm_source,
       gender,
       location_city,
       device,
       total_value,
        goal_name,
      toUnixTimestamp(toStartOfDay(at)) AS time_stamp
FROM events_campaigns
WHERE tenant_id = 1
    AND utm_campaign = 'c1'
    AND goal_name = 'g1'
    AND at >= FROM_UNIXTIME(1577836800)
    AND at <= FROM_UNIXTIME(1578009655)
;


-----------------Fake data for Truong.
insert into events_campaign values
(randomPrintableASCII(5),1, 'a6', 'click', randomPrintableASCII(3), randomPrintableASCII(3),
 'c1', 'content1', 'source2', 'Female', 'HCM', 'Mobile', NULL, NULL, NULL, '2020-01-01 04:05:00');