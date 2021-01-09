select *
from attribution_views;



-- conversion
with `dimension.names`[indexOf(`dimension.keys`,'dimension_filter')] as dimension_filter,
    `dimension.names`[indexOf(`dimension.keys`,'dimension_filter_value')] as dimension_filter_value,
    `dimension.names`[indexOf(`dimension.keys`,'channel')] as channel
select  channel,
        value_key,
        sum(value_value) as value_value
from attribution_views
where tenant_id = 1
    and window = 7
    and toUnixTimestamp(execution_time) >= 0
    and toUnixTimestamp(execution_time) <= 9999999999
    and model_name = 'linear'
    and whole_journey = 1
    and include_direct = 1
    and dimension_filter = 'campaign_grouping'
    and dimension_filter_value = 'cdp'
group by channel, value_key;


-- visit



---
select `dimension.names`[indexOf(`dimension.keys`,'dimension_filter_value')] as dimension_filter_value
from attribution_views
;

truncate table attribution_views;

insert into attribution_views values
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','cdp','facebook','c1'],'revenue',120),
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','cdp','facebook','c2'],'revenue',120),
 (1,'cf','cd1',now(),now(),7, 1,1,'linear',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','non_cdp','google','c1'],'revenue',120),
 (1,'cf','cd1',now(),now(),7, 1,1,'linear',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','non_cdp','google','c2'],'revenue',120);


insert into attribution_views values
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','cdp','facebook','c1'],'conversion',120),
(1,'cf','cd1',now(),now(),7, 1,1,'linear',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','cdp','facebook','c2'],'revenue',120),
 (1,'cf','cd1',now(),now(),7, 1,1,'linear',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','non_cdp','google','c1'],'conversion',120),
 (1,'cf','cd1',now(),now(),7, 1,1,'linear',['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','non_cdp','google','c2'],'revenue',120);

insert into attribution_views values
(1,'cf','cd1',now(),now(),7, 1,1,NULL,['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','cdp','facebook','c1'],'visit',320);

insert into attribution_views values
(1,'cf','cd1',now(),now(),7, 1,1,NULL,['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','cdp','facebook','c2'],'visit',320);

insert into attribution_views values
(1,'cf','cd1',now(),now(),7, 1,1,NULL,['dimension_filter','dimension_filter_value','channel','campaign'], ['campaign_grouping','cdp','google','c1'],'visit',320);

show create table attribution_views;

drop table attribution_views;
CREATE TABLE eventify_stag.attribution_views
(
    `tenant_id` UInt16,
    `config_pipeline_id` String,
    `config_data_id` String,
    `execution_time` DateTime,
    `created_at` DateTime,
    `window` Nullable(UInt8),
    `whole_journey` Nullable(UInt8),
    `include_direct` Nullable(UInt8),
    `model_name` Nullable(String),
    `dimension.keys` Array(String),
    `dimension.names` Array(String),
    `value_key` String,
    `value_value` Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(execution_time)
ORDER BY (tenant_id, execution_time)
;


select * from events_attribution_test;






