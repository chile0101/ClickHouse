
select count() from events_attribution_test;
select * from events_attribution_test;

show create table events_attribution_test;

drop table events_attribution_test;
CREATE TABLE eventify_stag.events_attribution_test
(
    `id` String,
    `tenant_id` UInt16,
    `anonymous_id` String,
    `event_name` String,
    `session_id` String,
    `scope` String,
    `identity.keys` Array(String),
    `identity.vals` Array(String),
    `str_properties.keys` Array(String),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(String),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(String),
    `arr_properties.vals` Array(Array(String)),
    `tag.keys` Array(String),
    `tag.vals` Array(String),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(at)
ORDER BY (tenant_id, anonymous_id, event_name, at);




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
      and at > '2020-01-01 00:00:00'
      and at < '2021-01-01 00:00:00'
      and tag.vals[indexOf(tag.keys, 'event_tag')] = 'conversion'

)
  and at > toDateTime('2020-12-10 08:59:48')
  and at < toDateTime('2020-12-17 08:59:48')
  and hasAny([campaign_grouping], ['cdp']) == 1
  and hasAny([touch_type],[]) == 0
  and hasAny([touch_type],  ['direct', 'paid', 'in-app', 'organic', 'conversion']) == 1
  and event_tag != 'system_event'
;