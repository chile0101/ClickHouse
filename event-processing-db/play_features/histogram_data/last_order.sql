show tables;

SELECT round((toUnixTimestamp(now()) - num_val) / ((24 * 60) * 60)) AS days
FROM profile_num_final_v
WHERE tenant_id = 1
        AND num_key = 'last_order_at'
        AND anonymous_id in (
                                SELECT user
                                FROM segment_user_final_v
                                WHERE tenant_id = 1 AND segment_id = '1js1xIzEc03e9XIdm34Iw9u1liz'
                            )
;


select * from segment_user_final_v;
select distinct num_key from profile_num;

round((toUnixTimestamp(now()) - t) / ((24 * 60) * 60)) AS days
;



