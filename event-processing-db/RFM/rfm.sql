select * from events_conversion;
select * from events_conversion where anonymous_id = 'DFYLDGTxu9BESsTgYRXBB3yFxcJ';

select * from rfm_metrics;
select * from profile_num_final_v;
select * from profile_str_final_v;

select now(), subtractDays(now(), 3);

--- get 30 60 90
select tenant_id,
       anonymous_id,
       sum(total_value) as monetary_value,
       count() as frequency,
       dateDiff('day', toDateTime(max(at)),now()) as recency
from events_conversion
where toDateTime(at) >= subtractDays(now(), 3) and toDateTime(at) <= now()
group by tenant_id, anonymous_id
;

--- RFM get lifetime

select tenant_id,
       anonymous_id,
       sum(total_value) as monetary_value,
       count() as frequency,
       dateDiff('day', toDateTime(max(at)),now()) as recency
from events_conversion
where toDateTime(at) <= now()
group by tenant_id, anonymous_id
;

------------


SELECT tenant_id,
       anonymous_id,
       sum(total_value) AS monetary_value,
       count() AS frequency,
       dateDiff('day',  toDateTime(max(at)), toDateTime('2020-12-06 04:40:58')) AS recency
FROM events_conversion
WHERE at <= '2020-12-06 04:40:58'
GROUP BY tenant_id, anonymous_id;




select * from profile_num_final_v where anonymous_id = 'DE5wbzZaX5jjUj9CFPNLavQlqT7';



select distinct event_name from events;

select * from events_conversion;

select * from rfm_metrics;

truncate table rfm_metrics;




SELECT u.tenant_id as tenant_id,
       u.anonymous_id as anonymous_id,
       e.total_value as monetary1,
       e.count as frequency1,
       toDate(u.at_final) as recency
FROM (
         WITH
             `num_properties.vals`[indexOf(`num_properties.keys`, '{EventsTablePropertyKeys.TOTAL_VALUE}')] AS value
         SELECT tenant_id,
                anonymous_id,
                sum(value) AS total_value,
                count() AS count,
                toDate(at) AS date
         FROM events
         WHERE event_name = '{EventNameEnum.CHECKOUT_COMPLETED}'
           AND at >= '2020-01-01 00:00:00'
           AND at <  '2020-11-20 00:00:00'
         GROUP BY tenant_id, anonymous_id, date
) AS e
RIGHT JOIN
(
     SELECT tenant_id,
            anonymous_id,
            max(toDateTime(at)) as at_final
     FROM profile_str_final_v
     GROUP BY tenant_id, anonymous_id
) AS u
ON e.tenant_id = u.tenant_id AND e.anonymous_id = u.anonymous_id
WHERE u.at_final < '2020-11-20 00:00:00'
;

select count(*) from profile_str_final_v ;
select count(*) from profile_num_final_v ;


select * from profile_str_final_v order by at desc ;
select * from profile_num_final_v order by at desc ;


select * from events;


