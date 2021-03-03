show tables ;




--------------------------------- TRUNCATE
truncate table events;
truncate table events_campaign;
truncate table events_conversion;
truncate table events_total_distinct_users_by_source_scope_ts;

truncate table profile_arr;
truncate table profile_arr_final;

truncate table profile_num;
truncate table profile_num_final;

truncate table profile_str;
truncate table profile_str_final;

truncate table segments;

truncate table segment_user;
truncate table segment_user_final;


truncate table segment_user_preview;

-- TRUNCATE REDIS: REDIS database 1
--
------------------------------ SELECT
select * from events where tenant_id = 1;
select * from profile_str;
select * from profile_str_final;
select * from profile_num;
select * from profile_num_final_v;


select * from segment_user_historical_queue;

select * from profile_num where anonymous_id = 'DKbVJXWCNRDaGgWsGujEXhn9Ajr';


show tables ;
