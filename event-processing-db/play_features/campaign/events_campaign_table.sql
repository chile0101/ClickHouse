# utm_medum : cach no trả tiền cho delivery


show tables;
select * from events_campaign limit 10;


drop table events_campaign;
CREATE TABLE events_campaign
(
    `id`                  String,
    `tenant_id`           UInt16,
    `anonymous_id`        String,
    `event_name`          String,
    `event_name_type`     String,
    `session_id`          String,
    `scope`               String,
    `campaign.keys`       Array(String),
    `campaign.vals`       Array(String),
    `identity.keys`       Array(String),
    `identity.vals`       Array(String),
    `str_properties.keys` Array(String),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(String),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(String),
    `arr_properties.vals` Array(Array(String)),
    created_date Date DEFAULT today(), -- '2020-01-01'
    created_at DateTime DEFAULT now()-- '2020-01-01 00:00:00'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(created_date)
ORDER BY (created_date, (tenant_id, id, anonymous_id, event_name_type, event_name,created_at))
;

select * from events_campaigns;
truncate table events_campaign;

--------------------------------------
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a1', 'click', 'engagement', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device'],['Male','HN','Phone'],[],[],[],[], '2020-01-01','2020-01-01 00:00:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a1', 'reach_goal','reach_go', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device','goal_name'],['Male','HN','Phone','g1'],[],[],[],[], '2020-01-01','2020-01-01 00:01:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a1', 'conversion', 'conversion',randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device'],['Male','HN','Phone'],['total_value'],[2],[],[], '2020-01-01','2020-01-01 00:03:00')
;

---------------------------------------
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a11', 'click','engagement', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device'],['Female','HCM','Phone'],[],[],[],[], '2020-01-01','2020-01-01 00:12:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a11', 'reach_goal','reach_go', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device','goal_name'],['Female','HCM','Phone','g1'],[],[],[],[], '2020-01-01','2020-01-01 01:01:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a11', 'conversion','conversion', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device'],['Female','HCM','Phone'],['total_value'],[5],[],[], '2020-01-01','2020-01-01 05:03:00')
;
--------------------------------------
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a2', 'click','engagement', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device'],['Male','HN','PC'],[],[],[],[], '2020-01-01','2020-01-01 07:00:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a2', 'reach_goal','reach_go', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device','goal_name'],['Male','HN','PC','g2'],[],[],[],[], '2020-01-01','2020-01-01 08:01:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a2', 'conversion','conversion', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device'],['Male','HN','PC'],['total_value'],[2],[],[], '2020-01-01','2020-01-01 10:03:00')
;
--------------------------------------
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a3', 'click','engagement', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device'],['Male','HN','PC'],[],[],[],[], '2020-01-01','2020-01-01 00:00:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a3', 'reach_goal','reach_go', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device','goal_name'],['Male','HN','PC','g2'],[],[],[],[], '2020-01-01','2020-01-01 00:01:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a3', 'conversion','conversion', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_1', 'facebook'], [],[],['gender','location_city','device'],['Male','HN','PC'],['total_value'],[2],[],[], '2020-01-01','2020-01-01 00:03:00')
;
----------------------------------------
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a4', 'click','engagement', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_2', 'google'], [],[],['gender','location_city','device'],['Male','HN','PC'],[],[],[],[], '2020-01-02','2020-01-02 00:00:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a4', 'reach_goal','reach_go', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_2', 'google'], [],[],['gender','location_city','device','goal_name'],['Male','HN','PC','g2'],[],[],[],[], '2020-01-02','2020-01-02 00:01:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a4', 'conversion','conversion', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_2', 'google'], [],[],['gender','location_city','device'],['Male','HN','PC'],['total_value'],[3],[],[], '2020-01-02','2020-01-02 00:03:00')
;

----------------------------------------
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a4', 'click','engagement', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_2', 'google'], [],[],['gender','location_city','device'],['Male','HCM','PC'],[],[],[],[], '2020-01-02','2020-01-02 12:00:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a4', 'reach_goal','reach_go', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_2', 'google'], [],[],['gender','location_city','device','goal_name'],['Male','HCM','PC','g3'],[],[],[],[], '2020-01-02','2020-01-02 12:01:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a4', 'conversion','conversion', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_2', 'google'], [],[],['gender','location_city','device'],['Male','HCM','PC'],['total_value'],[3],[],[], '2020-01-02','2020-01-02 12:03:00')
;
-----------------------------------------
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a5', 'click','engagement', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_3', 'google'], [],[],['gender','location_city','device'],['Male','HCM','PC'],[],[],[],[], '2020-01-02','2020-01-02 12:00:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a5', 'reach_goal','reach_go', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_3', 'google'], [],[],['gender','location_city','device','goal_name'],['Male','HCM','PC','g3'],[],[],[],[], '2020-01-02','2020-01-02 12:01:00')
;
insert into events_campaign values
(randomPrintableASCII(3), 1, 'a5', 'conversion','conversion', randomPrintableASCII(3), randomPrintableASCII(3), ['utm_campaign','utm_content','utm_source'],['c1','content_3', 'google'], [],[],['gender','location_city','device'],['Male','HCM','PC'],['total_value'],[3],[],[], '2020-01-02','2020-01-02 12:03:00')
;










-- INSERT INTO events_campaign
-- WITH
--     ['conversion'] AS event_names,
--     ['Iphone X', 'Xiaomi'] AS products,
--     ['Laptop','Mobile'] AS devices,
--     ['ss1','ss2','ss3'] AS sessions,
--     ['IOS-1gazgMAxIp60M6k9YE3HdghgRda', 'ADR-1hARTg5ZfLb9sauXFer0WR3V8W5','IOS-1glfxhzObkhKJqsQb0u8XhAn86Z'] as sources,
--     ['campaign1','campaign2','campaign3','campaign4','campaign5'] AS utm_campaigns,
--     ['facebook', 'google', 'mailchimp'] as utm_sources,
--     ['cpc', 'cpm'] as utm_mediums,
--     ['content1','content2','content3','content4','content5'] as utm_contents,
--     ['Male', 'Female'] as genders,
--     ['HaNoi', 'HoChiMinh','DaNang'] as locations
-- SELECT
--     rand(1)%10000 AS id,
--     1 AS tenant_id,
--     concat('a', toString(number)) AS anonymous_id,
--     event_names[(rand(2) % length(event_names)) + 1] AS event_name,
--     sessions[rand(3)%length(sessions) + 1] AS session_id,
--     sources[rand(4)%length(sources) + 1] AS scope,
--
--     ['user_id'] as `identity.keys`,
--     [rand(5)%10+1] as `identity.vals`,
--     ['currency','product', 'device','gender', 'location','utm_campaign','utm_content','utm_source'] AS `str_properties.keys`,
--     [   'VND',
--         products[(rand(6) % length(products)) + 1],
--         devices[rand(7) % length(devices) + 1],
--         genders[rand(8) % length(genders) + 1],
--         locations[rand(9) % length(locations) + 1],
--         utm_campaigns[rand(10) % length(utm_campaigns) + 1],
--         utm_sources[rand(11) % length(utm_sources) + 1],
--         utm_contents[rand(12) % length(utm_contents) + 1]] AS `str_properties.vals`,
--     ['total_value'] AS `num_properties.keys`,
--     [number] AS `num_properties.vals`,
--     [''] AS `arr_properties.keys`,
--     [[]] AS `arr_properties.vals`,
--     toDateTime('2020-01-02 00:00:00') + number * rand(13)%60 AS at
-- FROM numbers(20)
-- ;

select * from events_campaign;
truncate table events_campaign;















-----------------------------------UPDATE----------BACKED CH FORMAT
drop table events_campaigns;
CREATE TABLE events_campaigns
(
    `id`                  String,
    `tenant_id`           UInt16,
    `anonymous_id`        String,
    `event_name`          String,
--     `event_type`          String,
    `session_id`          String,
    `scope`               String,

    `utm_campaign`        String,
    `utm_content`         String,
    `utm_source`          String,

    `gender`              String,
    `location_city`       String,
    `device`             String,

    `goal_name`         Nullable(String),

    `total_value`          Nullable(String),
    `currency`             Nullable(String),

    `at`                  DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, anonymous_id, utm_campaign, event_name,at)
;



truncate table events_campaigns;

INSERT INTO events_campaigns
WITH
    ['conversion'] AS event_names,
    ['Laptop','Mobile'] AS devices,
    ['ss1','ss2','ss3'] AS sessions,
    ['IOS-1gazgMAxIp60M6k9YE3HdghgRda','IOS-1glfxhzObkhKJqsQb0u8XhAn86Z'] as sources,

    ['campaign1','campaign2'] AS utm_campaigns,
    ['facebook', 'google'] as utm_sources,
    ['cpc', 'cpm'] as utm_mediums,
    ['content1','content2'] as utm_contents,

     ['g1','g2','g3'] as goal_names,

    ['Male', 'Female'] as genders,
    ['HaNoi', 'HoChiMinh'] as locations
SELECT
    rand(1)%10000 AS id,
    1 AS tenant_id,
    concat('a', toString(1)) AS anonymous_id,
    event_names[(rand(2) % length(event_names)) + 1] AS event_name,
    sessions[rand(3)%length(sessions) + 1] AS session_id,
    sources[rand(4)%length(sources) + 1] AS scope,

    utm_campaigns[rand(5)%length(utm_campaigns) + 1] AS utm_campaign,
    utm_contents[rand(7)%length(utm_contents)+1] AS utm_content,
    utm_sources[rand(6)%length(utm_sources) + 1] AS utm_source,


    genders[rand(8)%length(genders) + 1] as gender,
    locations[rand(9)%length(locations) + 1] as location_city,
    devices[rand(10)%length(devices) + 1] as device,

    NULL as goal_name,

    number + 1 as total_value,
    'VND' as currency,

    toDateTime('2020-01-01 03:12:00') + number * rand(13)%60 AS at
FROM numbers(3)
;
------------------------------------------------------TEST
select
       utm_campaign,
       utm_content,
       utm_source,
       device,
       anonymous_id,
       total_value
from events_campaigns
where utm_campaign = 'campaign1'
    and event_name = 'click'

   utm_campaign utm_content utm_source user_trait  kpi_net_revenue  kpi_num_of_customers  kpi_num_of_orders  avg_revenue_per_user  kpi_avg_value_per_order
0    campaign1    content1   facebook                         3.0                   2.0                  2              1.500000                 1.500000
1    campaign1    content1   facebook     Laptop              2.0                   1.0                  1              2.000000                 2.000000
2    campaign1    content1   facebook     Mobile              5.0                   3.0                  3              1.666667                 1.666667
3    campaign1    content1     google                         5.0                   2.0                  2              2.500000                 2.500000
4    campaign1    content1     google     Mobile              4.0                   2.0                  2              2.000000                 2.000000
5    campaign1    content2   facebook     Laptop              1.0                   1.0                  1              1.000000                 1.000000
6    campaign1    content2   facebook     Mobile              3.0                   1.0                  1              3.000000                 3.000000
7    campaign1    content2     google     Laptop              5.0                   2.0                  2              2.500000                 2.500000
8    campaign1    content2     google     Mobile              3.0                   1.0                  1              3.000000                 3.000000



-------------------------------------------------------



INSERT INTO events_campaigns
WITH
    ['click'] AS event_names,
    ['Laptop','Mobile'] AS devices,
    ['ss1','ss2','ss3'] AS sessions,
    ['IOS-1gazgMAxIp60M6k9YE3HdghgRda','IOS-1glfxhzObkhKJqsQb0u8XhAn86Z'] as sources,

    ['campaign1','campaign2'] AS utm_campaigns,
    ['facebook', 'google'] as utm_sources,
    ['cpc', 'cpm'] as utm_mediums,
    ['content1','content2'] as utm_contents,

     ['g1','g2','g3'] as goal_names,

    ['Male', 'Female'] as genders,
    ['HaNoi', 'HoChiMinh'] as locations
SELECT
    rand(1)%10000 AS id,
    1 AS tenant_id,
    concat('a', toString(number)) AS anonymous_id,
    event_names[(rand(2) % length(event_names)) + 1] AS event_name,
    sessions[rand(3)%length(sessions) + 1] AS session_id,
    sources[rand(4)%length(sources) + 1] AS scope,

    utm_campaigns[rand(5)%length(utm_campaigns) + 1] AS utm_campaign,
    utm_contents[rand(7)%length(utm_contents)+1] AS utm_content,
    utm_sources[rand(6)%length(utm_sources) + 1] AS utm_source,


    genders[rand(8)%length(genders) + 1] as gender,
    locations[rand(9)%length(locations) + 1] as location_city,
    devices[rand(10)%length(devices) + 1] as device,

    goal_names[rand(11)%length(goal_names) + 1] as goal_name,

    NULL as total_value,
    NULL as currency,

    toDateTime('2020-01-01 00:00:00') + number * rand(13)%60 AS at
FROM numbers(3)
;

select * from events_campaigns;
show create table events_campaigns;







SELECT tenant_id,
       anonymous_id,
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
        AND utm_campaign = 'campaign1'
        AND at >= FROM_UNIXTIME(1577836800)
        AND at <= FROM_UNIXTIME(1578009655)
;


CREATE TABLE events_campaign_backup
(
    `id`                  String,
    `tenant_id`           UInt16,
    `anonymous_id`        String,
    `event_name`          String,
    `event_name_type`     String,
    `session_id`          String,
    `scope`               String,
    `campaign.keys`       Array(String),
    `campaign.vals`       Array(String),
    `identity.keys`       Array(String),
    `identity.vals`       Array(String),
    `str_properties.keys` Array(String),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(String),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(String),
    `arr_properties.vals` Array(Array(String)),
    created_date Date DEFAULT today(), -- '2020-01-01'
    created_at DateTime DEFAULT now()-- '2020-01-01 00:00:00'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(created_date)
ORDER BY (created_date, (tenant_id, id, anonymous_id, event_name_type, event_name,created_at))
;


drop table events_campaign_backup;

CREATE TABLE eventify.events_campaign
(
    `id`                  String,
    `tenant_id`           UInt16,
    `event_name`          String,
    `kafka_topic`         String,
    `anonymous_id`        String,
    `session_id`          String,
    `scope`               String,
    `utm_campaign`        String,
    `utm_medium`          Nullable(String),
    `utm_source`          Nullable(String),
    `utm_content`         Nullable(String),
    `utm_term`            Nullable(String),
    `str_properties.keys` Array(LowCardinality(String)),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(LowCardinality(String)),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(LowCardinality(String)),
    `arr_properties.vals` Array(Array(String)),
    `at`                  DateTime64(3),
    `at_unix`             UInt64
)
    ENGINE = MergeTree()
        PARTITION BY toYYYYMM(at)
        ORDER BY (tenant_id, utm_campaign, event_name, at);


select * from events_campaign_backup;







insert into events_campaign_backup select id,tenant_id, event_name, kafka_topic,
                                          anonymous_id, session_id, scope,
                                          utm_campaign,
                                          ec.utm_source as "utm_medium",
                                          ec.utm_content as "utm_source",
                                          ec.utm_medium as "utm_content",
     utm_term             ,
     str_properties.keys  ,
     str_properties.vals  ,
     num_properties.keys  ,
     num_properties.vals  ,
     arr_properties.keys  ,
     arr_properties.vals  ,
     at                   ,
     at_unix
from events_campaign ec;



select * from events_campaign_backup;

drop table events_campaign;


insert into events_campaign select * from events_campaign_backup;



-- source + channel + tactic
select distinct(utm_source) from eventify.events_campaign; -- lay medium - cpm  as medium

select distinct(utm_medium) from eventify.events_campaign; -- content-tactic - content as content

select distinct(utm_content) from eventify.events_campaign; -- content-tactic - channel as channel
select distinct(utm) from eventify.events_campaign; -- content-tactic - channel as channel


select distinct (utm_campaign) from events_campaign;



alter table events_campaign delete where utm_campaign != 'ARPU Increase Experiment';


select * from events_campaign where utm_campaign != 'ARPU Increase Experiment';



select * from events_campaign where