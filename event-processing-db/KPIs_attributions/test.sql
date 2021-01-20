show tables;

select *
from events_attribution_test;


select count() > 0
from events_attribution_test;


-------------------LOAD DATA

select tenant_id,
       anonymous_id                                                                    as "universal_id",
       id                                                                              as "touch_id",
       tag.vals[indexOf(tag.keys, 'source')]                                           as "touch_type",
       toUnixTimestamp(at)                                                             as "touch_timestamp",
       if(`tag.vals`[indexOf(`tag.keys`, 'event_tag')] == 'conversion', 1, 0)          as "is_conversion",
       event_name                                                                      as "touch_name",
       `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] as "conversion_value",
       tag.vals[indexOf(`tag.keys`, 'event_tag')]                                      as "event_tag",
       `str_properties.vals`[indexOf(`str_properties.keys`, 'campaign_source')]        as "channel",
       `str_properties.vals`[indexOf(`str_properties.keys`, 'campaign_name')]          as "campaign",
       `tag.vals`[indexOf(`tag.keys`, 'campaign_grouping')]                            as "campaign_grouping"
from events_attribution_test
where tenant_id = 1
  and anonymous_id in (
    select anonymous_id as "univeral_id"
    from events_attribution_test
    where tenant_id = 1
      and tag.vals[indexOf(tag.keys, 'event_tag')] = 'conversion'
      and at > '2020-01-01 00:00:00'
      and at < '2021-01-01 00:00:00'
)
  and event_tag != 'system_event'
  and at > '2020-25-12 00:00:00'
  and at < '2021-01-01 00:00:00'
  and hasAny([campaign_grouping], ['cdp']) == 1
  and hasAny([touch_type], ['direct']) == 0
  and hasAny([touch_type], ['direct', 'paid', 'in-app', 'organic', 'conversion']) == 1
;


select tenant_id,
       anonymous_id                                                                    as "universal_id",
       id                                                                              as "touch_id",
       tag.vals[indexOf(tag.keys, 'source')]                                           as "touch_type",
       toUnixTimestamp(at)                                                             as "touch_timestamp",
       if(`tag.vals`[indexOf(`tag.keys`, 'event_tag')] == 'conversion', 1, 0)          as "is_conversion",
       event_name                                                                      as "touch_name",
       `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] as "conversion_value",
       tag.vals[indexOf(`tag.keys`, 'event_tag')]                                      as "event_tag",
       `str_properties.vals`[indexOf(`str_properties.keys`, 'campaign_source')]        as "channel",
       `str_properties.vals`[indexOf(`str_properties.keys`, 'campaign_name')]          as "campaign",
       `tag.vals`[indexOf(`tag.keys`, 'campaign_grouping')]                            as "campaign_grouping"
from events_attribution_test
where tenant_id = %(tenant_id)s
  and anonymous_id in (
    select anonymous_id as "univeral_id"
    from events_attribution_test
    where tenant_id = %(tenant_id)s
      and tag.vals[indexOf(tag.keys, 'event_tag')] = 'conversion'
      and at > %(timerange_start)s
      and at < %(timerange_end)s
)
  and event_tag != 'system_event'
  and at > %(window_start_time)s
  and at < %(window_end_time)s
  and hasAny([campaign_grouping], %(campaign_filter)s) == 1
  and hasAny([touch_type], %(direct_filter_out)s) == 0
  and hasAny([touch_type], %(touch_type_filter)s) == 1
;

select * from attribution_views where value_key = 'visits';
truncate table attribution_views;



-----------API
select * from attribution_views;

 with `dimension.names`[indexOf(`dimension.keys`,'dimension_filter')] as dimension_filter,
        `dimension.names`[indexOf(`dimension.keys`,'dimension_filter_value')] as dimension_filter_value,
        `dimension.names`[indexOf(`dimension.keys`,'channel')] as dimension
    select
        dimension,
        value_key,
        sum(value_value) as value_value
    from attribution_views
    where config_data_id = '1.1577836800.1609459200.all_traffic.cdp.linear.7.1'
    group by dimension, value_key


--------------


select * from events_attribution_test;