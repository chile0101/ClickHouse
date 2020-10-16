show tables;

create database eventify_demo;

show tables;

show create table event_total_user_by_source_ts;
show create table event_total_user_by_source_ts_mv;

show create table event_total_event_name_by_source_daily;
show create table event_total_event_name_by_source_daily_mv;



CREATE MATERIALIZED VIEW eventify.event_total_event_name_by_source_daily_mv TO eventify.event_total_event_name_by_source_daily
(
    `day` Date,
    `tenant_id` UInt16,
    `event_name` String,
    `source_id` String,
    `total` UInt64
) AS
SELECT
    toDate(at) AS day,
    tenant_id,
    event_name,
    str_properties.vals[indexOf(str_properties.keys, 'source.scope')] AS source_id,
    count() AS total
FROM eventify.events
GROUP BY (day, tenant_id, event_name, source_id);


show tables ;

drop table campaign_total_revenue;

show create event_total_event_name_by_source_daily_mv;
