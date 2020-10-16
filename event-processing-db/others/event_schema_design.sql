CREATE TABLE events
(
    event_id String,
    timestamp Date,
    event_type String,
    user_id String,
    NumProperties Nested
    (
        numeric_prop_keys String,
        numeric_prop_vals UInt32
    ),
    StrProperties Nested
    (
        string_prop_keys String,
        string_prop_vals String
    )
)Engine=MergeTree(timestamp,event_id,8192);


INSERT INTO events VALUES 
('001', '2017-01-01','click','user1',['nkey1','nkey2','nkey3'],[1,2,3],['skey1','skey2'],['svalue1','svalue2'])

-- Not support 
select *
from events
array join NumProperties
array join StrProperties 

-- Must be
select * from 
(select * from events array join NumProperties)
array join StrProperties



















-- Entities
create table events(
    `id` String,
    `tenant_id` UInt16, 
    `at` Datetime,
    `name` String,
    `user_id` String,
    `session_id` String,
    `scope` String,
    `num_properties` Nested(
        keys LowCardinality(String),
        vals Float32
    ),
    `str_properties` Nested(
        keys LowCardinality(String),
        vals String
    ),
    `arr_properties` Nested(
        keys LowCardinality(String),
        vals Array(String)
    )
)ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, name, toStartOfHour(at))
SETTINGS index_granularity = 8192

-- why order by toStartOfHour(at), at

create table eventify.segments_users(
    tenant String,
    user_id String,
    effective DateTime,
    expired DateTime,
    current UInt32,
    segment_id String
)Engine MergeTree()
Order By (tenant, user_id, segment_id, effective)
SETTINGS index_granularity=8192;
-- effective, expired, current, segment_id

----
create table eventify.users(
    tenant String,
    user_id String,
    anonymous_id String,
    identifies ARRAY(String),

    context_ip String,
    context_user_agent ARRAY(String),
    
    integrations Nested(
        integration_keys LowCardinality(String),
        integration_vals UInt8
    ),
    str_properties Nested(
        keys String,
        vals String
    ),
    num_properties Nested(
        keys String,
        vals Float32
    ),

    create_at DateTime,
    expired_at DateTime,
    current UInt32
) Engine MergeTree()
Order By (tenant, user_id, create_at)
SETTINGS index_granularity = 8192;
-- identifies, context_ip, context_user_agent, integrations
-- why primary key / order by


create table eventify.segments (
    id String, 
    state String,
    user String,
    at Datetime
)Engine = MergeTree()
Order by (at, id)
PARTITION BY(id) 
index_granularity=8192;