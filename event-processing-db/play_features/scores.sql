select tenant_id,
       anonymous_id,
       total_order,
       dateDiff('day', toDate('2020-01-01 00:00:00'), day) as date_diff
from
(
    select tenant_id,
           anonymous_id,
           toDate(at) as day,
           count() as total_order
    from events_conversion
    where toDateTime(at) <= '2020-12-29 16:15:57'
        and toDateTime(at) >= subtractDays(toDateTime('2020-12-29 16:15:57'), 30)
    group by tenant_id, anonymous_id, day
    order by tenant_id, anonymous_id, day
)

;


select * from events_conversion;


select *
from profile_num_final_v
where anonymous_id = 'DET9BOMWtFGk6Gar0e6jZ0r8Iak';


SELECT tenant_id,
       anonymous_id,
       total_order,
       dateDiff('day', toDate('2020-01-01 00:00:00'), day) AS day_number
FROM
(
    SELECT tenant_id,
           anonymous_id,
           toDate(at) AS day,
           COUNT() AS total_order
    FROM events_conversion
    WHERE toDateTime(at) <= now()
        AND toDateTime(at) >= subtractDays(now(), 30)
    GROUP BY tenant_id, anonymous_id, day
    ORDER BY tenant_id, anonymous_id, day
)
ORDER BY tenant_id, anonymous_id, day_number
;
----------------------------------------------------- campaign engagement score
select * from events_campaign;


select tenant_id, anonymous_id , count() from (
                                               SELECT tenant_id,
                                                      anonymous_id,
                                                      campaign_engagement,
                                                      dateDiff('day', toDate('2020-01-01 00:00:00'), day) AS day_number
                                               FROM (
                                                     SELECT tenant_id,
                                                            anonymous_id,
                                                            toDate(at) AS day,
                                                            COUNT()    AS campaign_engagement
                                                     FROM events_campaign
                                                     WHERE at <= '2020-12-30 06:48:55'
                                                       AND at >= subtractDays(toDateTime('2020-12-30 06:48:55'), 60)
                                                     GROUP BY tenant_id, anonymous_id, day
                                                     ORDER BY tenant_id, anonymous_id, day
                                                        )
                                               ORDER BY tenant_id, anonymous_id, day_number

                                                  )
group by tenant_id, anonymous_id
having count() > 1
;

select * from profile_num_final_v where anonymous_id = 'DDG0m1j9ABZ9AJzpRe6qVLmBtEx';
frequency1
frequency30
frequency60
frequency90
monetary1
monetary30
monetary60
monetary90
recency
frequency1
frequency30
frequency60
frequency90
monetary1
monetary30
monetary60
monetary90
recency
;

select distinct num_key from profile_num_final_v;
select distinct str_key from profile_str_final_v;


SELECT tenant_id,
       anonymous_id,
       sum(total_value) as monetary_value,
       count() as frequency,
       dateDiff('day', toDateTime(max(at)), toDateTime(now())) AS recency
FROM events_conversion
WHERE toDateTime(at) >= subtractDays(now(), 10)
      and at <= now64()
GROUP BY tenant_id, anonymous_id;

select * from events_conversion;


