show tables ;

drop table segments;
drop table segments_final;
drop table segments_final_mv;

drop table users;
drop table users_final;
drop table users_final_mv;
drop table users_final_v;

drop table segment_agg_days_since_last_order_mv;
drop table segment_agg_final;
drop table segment_agg_final_mv;
drop table segment_agg_final_v;
drop table segment_agg_gender_mv;
drop table segment_agg_locatoin_city_mv;
drop table segment_agg_revenue_mv;
drop table segment_agg_source_mv;

drop table event_total_event_name_by_source_daily;
drop table event_total_event_name_by_source_daily_mv;
drop table event_total_user_by_source_daily;
drop table event_total_user_by_source_daily_mv;

drop table user_segments_v;
drop table events;
drop table segment_agg;
drop table segment_users;
drop table user_profile;
drop table user_profile_final;
drop table user_profile_final_mv;
drop table user_profile_final_v;


SELECT
    tenant_id,
    segment_id,
    length(users) as total_user
FROM
(
    SELECT
        tenant_id,
        segment_id,
        argMaxMerge(users) AS users
    FROM segment_users_final
    WHERE (tenant_id = 1) AND (segment_id = 's1')
    GROUP BY
        tenant_id,
        segment_id
)
;

insert into segment_users values
(1, 's1', ['a1','a2'], now())
;

select * from segment_agg_final_v;

select * from user_profile_final;







