drop table segment_user_historical_queue;
create table segment_user_historical_queue
(
    segment_id String,
    tenant_id  UInt16,
    user       String,
    state      String,
    at         DateTime64(3)
)
    engine = Kafka()
        SETTINGS kafka_broker_list =
                '10.110.1.5:9092, 10.110.1.5:9093,10.110.1.5:9094', kafka_topic_list = 'segment-user-historical-update', kafka_group_name = 'segment_user_historical_group', kafka_format = 'JSONEachRow', kafka_num_consumers = 1;

create table segment_user_preview
(
    segment_id String,
    tenant_id  UInt16,
    user       String,
    status     UInt8,
    at         DateTime64(3)
)
    engine = MergeTree()
        PARTITION BY toYYYYMM(at)
        ORDER BY (tenant_id, segment_id, user, at)
        SETTINGS index_granularity = 8192;

create table segments
(
    tenant_id       UInt16,
    id              String,
    name            String,
    description     String,
    type            String,
    status          String,
    conditions      String,
    mark_as_deleted Int8,
    at              DateTime64(3) default now64()
)
    engine = MergeTree()
        PARTITION BY toYYYYMM(at)
        ORDER BY (tenant_id, id, at)
        SETTINGS index_granularity = 8192;



CREATE VIEW campaign_data_joined_v
            (
             `tenant_id` UInt16,
             `anonymous_id` String,
             `event_name` String,
             `event_name_type` String,
             `utm_campaign` String,
             `utm_content` Nullable(String),
             `utm_source` Nullable(String),
             `total_value` Float64,
             `reach_goal_id` String,
             `gender` String,
             `location` String,
             `device` String,
             `at` DateTime
                )
AS
SELECT tenant_id,
       anonymous_id,
       event_name,
       event_name_type,
       utm_campaign,
       utm_content,
       utm_source,
       total_value,
       reach_goal_id,
       gender,
       location,
       device,
       at
FROM (
         SELECT tenant_id,
                anonymous_id,
                event_name,
                event_name_type,
                utm_campaign,
                utm_content,
                utm_source,
                `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] AS total_value,
                `str_properties.vals`[indexOf(`str_properties.keys`, 'reach_goal_id')]          AS reach_goal_id,
                at
         FROM events_campaign
         ) AS ec
         INNER JOIN
     (
         SELECT tenant_id,
                anonymous_id,
                str_vals[indexOf(str_keys, 'gender')]          AS gender,
                str_vals[indexOf(str_keys, 'city')]            AS location,
                str_vals[indexOf(str_keys, 'device_platform')] AS device
         FROM (
               SELECT tenant_id,
                      anonymous_id,
                      groupArray(str_key) AS str_keys,
                      groupArray(str_val) AS str_vals
               FROM profile_str_final_v
               GROUP BY tenant_id,
                        anonymous_id
                  )
         ) AS pf ON (ec.tenant_id = pf.tenant_id) AND (ec.anonymous_id = pf.anonymous_id)
ORDER BY tenant_id ASC,
         utm_campaign ASC,
         event_name_type ASC,
         at ASC;



CREATE VIEW profile_v
            (
             `tenant_id` UInt16,
             `anonymous_id` String
                )
AS
SELECT coalesce(ps.tenant_id, pn.tenant_id)       AS tenant_id,
       coalesce(ps.anonymous_id, pn.anonymous_id) AS anonymous_id
FROM (
         SELECT tenant_id,
                anonymous_id
         FROM profile_str
         GROUP BY tenant_id,
                  anonymous_id
         ) AS ps
         FULL OUTER JOIN
     (
         SELECT tenant_id,
                anonymous_id
         FROM profile_num
         GROUP BY tenant_id,
                  anonymous_id
         ) AS pn ON (ps.tenant_id = pn.tenant_id) AND (ps.anonymous_id = pn.anonymous_id);


CREATE VIEW segments_v
            (
             `tenant_id` UInt16,
             `segment_id` String,
             `name` String,
             `description` String,
             `type` String,
             `status` String,
             `conditions` String,
             `mark_as_deleted` Int8,
             `at_final` DateTime64(3)
                )
AS
SELECT tenant_id,
       id                          AS segment_id,
       argMax(name, at)            AS name,
       argMax(description, at)     AS description,
       argMax(type, at)            AS type,
       argMax(status, at)          AS status,
       argMax(conditions, at)      AS conditions,
       argMax(mark_as_deleted, at) AS mark_as_deleted,
       max(at)                     AS at_final
FROM segments
GROUP BY tenant_id,
         id;