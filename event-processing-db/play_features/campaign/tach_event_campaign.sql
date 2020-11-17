show tables;
select * from events_campaign limit 20;
show create events_campaign;


CREATE TABLE eventify.events_campaign_new
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
    `at`                  DateTime,
    `at_milli`            DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, utm_campaign, event_name, at);





----- conversion
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

    `utm_campaign`        String DEFAULT '',
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
    `at`                  DateTime,
    `at_milli`            DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, utm_campaign, anonymous_id, at);

select * from events_conversion;
--
insert into events_conversion
select id,
       tenant_id,
       event_name,
       anonymous_id,
       session_id,
       scope,

              num_properties.vals[indexOf(num_properties.keys, 'properties.total_value')] as total_value,
       str_properties.vals[indexOf(str_properties.keys, 'properties.currency')] as currency,
        num_properties.vals[indexOf(num_properties.keys, 'properties.discount_value')] as discount_value,

       utm_campaign,
       utm_medium,
       utm_source,
       utm_content,
       utm_term,

        str_properties.keys,
       str_properties.vals,
       num_properties.keys,
       num_properties.vals,
       arr_properties.keys,
       arr_properties.vals,
       e.at as at_milli,
       toDateTime(e.at) as at
from events_campaign as e
where kafka_topic = 'conversion';










-- event_reach_goal
CREATE TABLE events_reach_goal
(
    `id`                  String,
    `tenant_id`           UInt16,
    `event_name`          String,
    `anonymous_id`        String,
    `session_id`          String,
    `scope`               String,

    `reach_goal_id`       String,

    `utm_campaign`        String DEFAULT '',
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
    `at`                  DateTime,
    `at_milli`            DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, utm_campaign,  reach_goal_id, anonymous_id, at);


select * from events_reach_goal;

insert into events_reach_goal
select id,
       tenant_id,
       event_name,
       anonymous_id,
       session_id,
       scope,

       str_properties.vals[indexOf(str_properties.keys, 'reach_goal_id')] as reach_goal_id,

       utm_campaign,
       utm_medium,
       utm_source,
       utm_content,
       utm_term,

        str_properties.keys,
       str_properties.vals,
       num_properties.keys,
       num_properties.vals,
       arr_properties.keys,
       arr_properties.vals,
       e.at as at_milli,
       toDateTime(e.at) as at
from events_campaign as e
where kafka_topic = 'reach_goal' and reach_goal_id != '' and utm_campaign != '';



-- events_session_updated
CREATE TABLE events_session_updated
(
    `id`                  String,
    `tenant_id`           UInt16,
    `event_name`          String,
    `anonymous_id`        String,
    `session_id`          String,
    `scope`               String,

    `utm_campaign`        String DEFAULT '',
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
    `at`                  DateTime,
    `at_milli`            DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, utm_campaign, event_name, anonymous_id, at);


select * from events_session_updated;
select * from events_campaign;

insert into events_session_updated
select id,
       tenant_id,
       event_name,
       anonymous_id,
       session_id,
       scope,

       utm_campaign,
       utm_medium,
       utm_source,
       utm_content,
       utm_term,

        str_properties.keys,
       str_properties.vals,
       num_properties.keys,
       num_properties.vals,
       arr_properties.keys,
       arr_properties.vals,
       e.at as at_milli,
       toDateTime(e.at) as at
from events_campaign as e
where kafka_topic = 'session_updated_topic';



show tables;
select * from events_campaign;

-- ARPU Increase Experiment




----------------------------------------Edit Sql
DDbhRtjk1B7W6MW6O304YE9B39C,1,checkout_completed,conversion,DDbhRMMzIGxLLWPDfpcyalEm2OY,89f3909e-7584-daa2-b3fb-d5d683c6cb40,JS-1iRy2Fu6ofUZ6KpGqHtp7w14Qej,ARPU Increase Experiment,Free Shipping,cpm,Facebook,,['properties.currency'],['VND'],"['properties.discount_value','properties.total_value','properties.shipping_cost']","[200000,1200000,12763]",[],[],2020-11-09 12:00:05.990,1604923205990

show create table events_campaign;
select * from events_campaign;
select * from events_session_updated;
select * from events_reach_goal;
select * from events_conversion;

    `id` String,
    `tenant_id` UInt16,
    `event_name` String,
    `kafka_topic` String,
    `anonymous_id` String,
    `session_id` String,
    `scope` String,
    `utm_campaign` String,
    `utm_medium` Nullable(String),
    `utm_source` Nullable(String),
    `utm_content` Nullable(String),
    `utm_term` Nullable(String),
    `str_properties.keys` Array(LowCardinality(String)),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(LowCardinality(String)),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(LowCardinality(String)),
    `arr_properties.vals` Array(Array(String)),
    `at` DateTime64(3),
    `at_unix` UInt64;



ARPU Increase Experiment,cpm,Facebook,30% Discount

----
select * from events_reach_goal;
insert into events_campaign
    select id, tenant_id, event_name, 'reach_goal' as kafka_topic, anonymous_id, session_id, scope,
           'ARPU Increase Experiment' as utm_campaign,
           'cpm' as utm_medium,
           'Facebook' as utm_source,
           '30% Discount' as utm_content,
           NULL as utm_term,
           str_properties.keys,
           str_properties.vals,
           num_properties.keys,
           num_properties.vals,
           arr_properties.keys,
           arr_properties.vals,
           now64(),
           toUnixTimestamp64Milli(now64()) as at_unix
    from events_reach_goal
        where length(str_properties.vals) != 0

-------------KPI
                                                            channel_clicks metric_type
utm_content   utm_source device gender location_city
30% Discount                                                   566.0    channels
              Facebook                                         656.0    channels
                                       Biên Hòa                 24.0    channels
                                       Bắc Cạn                  19.0    channels
                                       Bắc Ninh                 18.0    channels
                                       Cao Bằng                 23.0    channels
                                       Cần Thơ                  25.0    channels
                                       Dắc Lăk                  22.0    channels
                                       Huế                      20.0    channels
                                       Hà Nội                   20.0    channels
                                       Hải Dương                20.0    channels
                                       Hải Phòng                19.0    channels
                                       Hồ Chí Minh              21.0    channels
                                       Long Xuyên               20.0    channels
                                       Mỹ Tho                   20.0    channels
                                       Nam Định                 45.0    channels
                                       Ninh Bình                20.0    channels
                                       Quy Nhơn                 23.0    channels
                                       Quảng Bình               17.0    channels
                                       Quảng Trị                29.0    channels
                                       Rạch Giá                 18.0    channels
                                       Thái Bình                25.0    channels
                                       Vĩnh Phúc                22.0    channels
                                       Điện Biên                19.0    channels
                                       Đà Lạt                    9.0    channels
                                       Đà Nẵng                  22.0    channels
                                       Đồng Hới                 16.0    channels
                                Female                         298.0    channels
                                Male                           238.0    channels
                         iOS                                   536.0    channels
Free Shipping                                                 1160.0    channels
              Facebook                                        1268.0    channels
                                       Biên Hòa                 47.0    channels
                                       Bắc Cạn                  26.0    channels
                                       Bắc Ninh                 36.0    channels
                                       Cao Bằng                 46.0    channels
                                       Cần Thơ                  59.0    channels
                                       Dắc Lăk                  47.0    channels
                                       Huế                      51.0    channels
                                       Hà Nội                   44.0    channels
                                       Hải Dương                36.0    channels
                                       Hải Phòng                54.0    channels
                                       Hồ Chí Minh              52.0    channels
                                       Long Xuyên               63.0    channels
                                       Mỹ Tho                   41.0    channels
                                       Nam Định                 78.0    channels
                                       Ninh Bình                64.0    channels
                                       Quy Nhơn                 35.0    channels
                                       Quảng Bình               44.0    channels
                                       Quảng Trị                35.0    channels
                                       Rạch Giá                 18.0    channels
                                       Thái Bình                40.0    channels
                                       Vĩnh Phúc                41.0    channels
                                       Điện Biên                45.0    channels
                                       Đà Lạt                   44.0    channels
                                       Đà Nẵng                  45.0    channels
                                       Đồng Hới                 33.0    channels
                                Female                         604.0    channels
                                Male                           520.0    channels
                         iOS                                  1124.0    channels

------------ REACH GOAL
['1k5hpbaohArW8JT4x64nAjRxtMk']


 ['utm_content', 'utm_source', 'device', 'gender', 'location_city', 'metric_type']
goal_action_completion metric_type
utm_content  utm_source device gender location_city
30% Discount                                                            2.0       goals
             Facebook                                                   2.0       goals
                                      Ninh Bình                         2.0       goals
                               Male                                     2.0       goals
                        iOS                                             2.0       goals




data_df

   tenant_id                 anonymous_id      event_name event_name_type              utm_campaign   utm_content utm_source  total_value                   time_stamp gender location_city device
0          1  DDgyOTOWGZfAwZcAI87kT6KklVI  product_viewed      reach_goal  ARPU Increase Experiment  30% Discount   Facebook          0.0  1k5hpbaohArW8JT4x64nAjRxtMk   Male     Ninh Bình    iOS
1          1  DDgyOTOWGZfAwZcAI87kT6KklVI  product_viewed      reach_goal  ARPU Increase Experiment  30% Discount   Facebook          0.0  1k5hpbaohArW8JT4x64nAjRxtMk   Male     Ninh Bình    iOS


data_df_pre_process
:    tenant_id                 anonymous_id      event_name event_name_type              utm_campaign   utm_content utm_source  total_value                   time_stamp gender location_city device
0          1  DDgyOTOWGZfAwZcAI87kT6KklVI  product_viewed      reach_goal  ARPU Increase Experiment  30% Discount   Facebook          0.0  1k5hpbaohArW8JT4x64nAjRxtMk   Male     Ninh Bình    iOS
1          1  DDgyOTOWGZfAwZcAI87kT6KklVI  product_viewed      reach_goal  ARPU Increase Experiment  30% Discount   Facebook          0.0  1k5hpbaohArW8JT4x64nAjRxtMk   Male     Ninh Bình    iOS


metric name: [goal id]
base columns =  [['utm_content'], ['utm_content', 'utm_source'], ['utm_content', 'utm_source', 'device'], ['utm_content', 'utm_source', 'gender'], ['utm_content', 'utm_source', 'location_city']]




drop table events_campaign;
CREATE TABLE eventify.events_campaign
(
    `id`                  String,
    `tenant_id`           UInt16,
    `event_name`          String,
    `event_name_type`     String,
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
    `at`                  DateTime,
    `at_milli`            DateTime64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, utm_campaign, event_name, at);

select * from eventify.events_campaign_new;

insert into eventify.events_campaign
select id, tenant_id, event_name, event_name_type, anonymous_id, session_id, scope, utm_campaign,
       utm_medium, utm_source, utm_content, utm_term,
       str_properties.keys, str_properties.vals,
       num_properties.keys, num_properties.vals,
       arr_properties.keys, arr_properties.vals,
       toDateTime(e.at) as at,
       e.at as at_milli
from events_campaign_new as e;


select * from events_campaign where anonymous_id = 'DDqSPxyhYGLdkT81uMmPn1MIKBk';



show create table events_campaign;


