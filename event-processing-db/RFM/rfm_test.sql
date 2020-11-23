show tables;

show create table rfm_metrics;
-- CREATE TABLE eventify_stag.rfm_metrics
-- (
--     `tenant_id` UInt16,
--     `anonymous_id` String,
--     `metrics.keys` Array(String),
--     `metrics.vals` Array(Float64),
--     `rfm.keys` Array(String),
--     `rfm.vals` Array(String),
--     `date` Date
-- )
-- ENGINE = MergeTree()
-- PARTITION BY toYYYYMMDD(date)
-- ORDER BY (date, tenant_id, anonymous_id)

select * from rfm_metrics;


show tables ;
show create table adj_graph;


select count(*) from profile_num;
select count(*) from profile_num_final_v;
select * from profile_num_final_v;



select count() from profile_num;

show create rfm_metrics;
select * from rfm_metrics;

['recency','frequency1','frequency30','frequency60','frequency90','monetary1','monetary30','monetary60','monetary90']
['rfm30','rfm60','rfm90']


------------ segment level

-- total revenue
-- total order
-- avg revenue per order
-- total customer

-- gender
-- location
-- channel

------------- user level

-- revenue
-- total order
-- avg order value
-- day since last order
-- order frequence score
-- order momentum score
-- order propensity score
-- churn propensity score
-- forecast LTV

--------------histogram


