show tables ;


select * from profile_str_final_v;

select count()
from (
     select tenant_id, anonymous_id
     from profile_str
     group by tenant_id, anonymous_id
);
;


CREATE VIEW profile_v AS
SELECT coalesce(ps.tenant_id, pn.tenant_id) AS tenant_id,
       coalesce(ps.anonymous_id, pn.anonymous_id) AS anonymous_id
FROM
    (
        SELECT tenant_id, anonymous_id
        FROM profile_str
        GROUP BY tenant_id, anonymous_id
    ) AS ps
    FULL JOIN
    (
        SELECT tenant_id, anonymous_id
        FROM profile_num
        GROUP BY tenant_id, anonymous_id
    ) AS pn
    ON ps.tenant_id = pn.tenant_id AND ps.anonymous_id = pn.anonymous_id
;

select * from profile_v;












show tables ;
show create table attribution_views;

--remove
segment_size
segment_total_user_ts
segment_total_user_ts_mv
rfm_metrics;

select * from segment_size;



select * from profile_str_final_v where anonymous_id = 'DIXB8s9Bn5xNmgEcaGnIAIyIM4X';
select * from