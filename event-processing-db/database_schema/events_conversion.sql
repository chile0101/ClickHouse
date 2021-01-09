CREATE TABLE events_conversion
(
    `id`                  String,
    `tenant_id`           UInt16,
    `event_name`          String,
    `anonymous_id`        String,
    `session_id`          String,
    `scope`               String,

    `total_value`         Float64,
    `currency`            String,
    `discount_value`      Nullable(Float32),

    `str_properties.keys` Array(LowCardinality(String)),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(LowCardinality(String)),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(LowCardinality(String)),
    `arr_properties.vals` Array(Array(String)),
    `at`
    `_timestamp`            DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at);


select  * from events_conversion;

select count() from events;

insert into events_conversion
select id,
       tenant_id,
       event_name,
       anonymous_id,
       session_id,
       scope,
       `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] as total_value,
       'VND' as currency,
       `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.discount')] as discount_value,

       `str_properties.keys`,
       `str_properties.vals`,
       `num_properties.keys`,
       `num_properties.vals`,
       `arr_properties.keys`,
       `arr_properties.vals`,
       at
from events
where total_value != 0
;




