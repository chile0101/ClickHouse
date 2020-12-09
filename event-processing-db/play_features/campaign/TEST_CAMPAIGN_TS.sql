select * from events_campaign where utm_campaign ='CAMPAIGN Facebook' and utm_content = '20% Discount' order by at desc ;


select toStartOfFifteenMinutes();

select toStartOfTenMinutes();

------------------------------------------------------


SELECT tenant_id,
       anonymous_id,
       event_name,
       event_name_type,
       utm_campaign,
       utm_content,
       utm_source,
        total_value,
       reach_goal_id
       time_stamp,
       gender,
       location,
       device
FROM
(
    SELECT
        tenant_id,
        anonymous_id,
        event_name,
        event_name_type,
        utm_campaign,
        utm_content,
        utm_source,
        `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] AS total_value,
        `str_properties.vals`[indexOf(`str_properties.keys`, 'reach_goal_id')] as reach_goal_id,
        toUnixTimestamp(toStartOfTenMinutes(at)) AS time_stamp
    FROM events_campaign
    WHERE tenant_id = 1
      AND utm_campaign = 'CAMPAIGN Google'
      AND toUnixTimestamp(at) >= 1606343467
      AND toUnixTimestamp(at) <= 1606582799
) AS ec
INNER JOIN
(
    SELECT
           tenant_id,
           anonymous_id,
           str_vals[indexOf(str_keys, 'gender')] AS gender,
           str_vals[indexOf(str_keys, 'city')] AS location,
           str_vals[indexOf(str_keys, 'device_platform')] AS device
    FROM
    (
        SELECT tenant_id,
               anonymous_id,
               groupArray(str_key) AS str_keys,
               groupArray(str_val) AS str_vals
        FROM profile_str_final_v
        WHERE tenant_id = 1
              AND  anonymous_id IN (SELECT anonymous_id
                                    FROM events_campaign
                                    WHERE tenant_id = 1
                                          AND utm_campaign = 'CAMPAIGN Google'
                                          AND toUnixTimestamp(at) >= 1606443467
                                          AND toUnixTimestamp(at) <= 1606582799 )
              AND str_key IN ('gender', 'city', 'device_platform')
         GROUP BY tenant_id, anonymous_id
    )
) AS pf
ON ec.tenant_id = pf.tenant_id AND ec.anonymous_id = pf.anonymous_id
;

select * from events_campaign where event_name_type ='conversion' order by at desc ;

---------------------------View


drop table campaign_performance_data;
CREATE VIEW campaign_performance_data AS
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
FROM
(
    SELECT
        tenant_id,
        anonymous_id,
        event_name,
        event_name_type,
        utm_campaign,
        utm_content,
        utm_source,
        `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] AS total_value,
        `str_properties.vals`[indexOf(`str_properties.keys`, 'reach_goal_id')] as reach_goal_id,
         at
    FROM events_campaign
) AS ec
INNER JOIN
(
    SELECT
           tenant_id,
           anonymous_id,
           str_vals[indexOf(str_keys, 'gender')] AS gender,
           str_vals[indexOf(str_keys, 'city')] AS location,
           str_vals[indexOf(str_keys, 'device_platform')] AS device
    FROM
    (
        SELECT tenant_id,
               anonymous_id,
               groupArray(str_key) AS str_keys,
               groupArray(str_val) AS str_vals
        FROM profile_str_final_v
        WHERE str_key IN ('gender', 'city', 'device_platform')
         GROUP BY tenant_id, anonymous_id
    )
) AS pf
ON ec.tenant_id = pf.tenant_id AND ec.anonymous_id = pf.anonymous_id
ORDER BY tenant_id, utm_campaign, event_name_type, at
;



---------net revenue
show create table campaign_performance_data;


select utm_content,
       sum(total_value)
from campaign_performance_data
where utm_campaign = 'CAMPAIGN Facebook'
    and event_name_type = 'conversion'
    and toUnixTimestamp(at) >= 1606064400 and toUnixTimestamp(at) <= 1606582799
group by utm_content
;

select utm_content,
       utm_source,
       sum(total_value)
from campaign_performance_data
where utm_campaign = 'CAMPAIGN Facebook'
    and event_name_type = 'conversion'
    and toUnixTimestamp(at) >= 1606064400 and toUnixTimestamp(at) <= 1606582799
group by utm_content, utm_source;


-----------------------------
select * from
(
select utm_content,
       utm_source,
       gender as ptrait,
       sum(total_value)
from campaign_performance_data
where utm_campaign = 'CAMPAIGN Facebook'
    and event_name_type = 'conversion'
    and toUnixTimestamp(at) >= 1606064400 and toUnixTimestamp(at) <= 1606582799
group by utm_content, utm_source, gender
    )
union all
(
    select utm_content,
           utm_source,
           device as ptrait,
           sum(total_value)
    from campaign_performance_data
    where utm_campaign = 'CAMPAIGN Facebook'
      and event_name_type = 'conversion'
      and toUnixTimestamp(at) >= 1606064400
      and toUnixTimestamp(at) <= 1606582799
    group by utm_content, utm_source, device

)
;

---------------------num of customers

select utm_content,
       count(distinct anonymous_id)
from campaign_performance_data
where utm_campaign = 'CAMPAIGN Facebook'
    and event_name_type = 'conversion'
    and toUnixTimestamp(at) >= 1606064400 and toUnixTimestamp(at) <= 1606582799
group by utm_content
order by utm_content
;


select utm_content,
       utm_source,
       count(distinct anonymous_id)
from campaign_performance_data
where utm_campaign = 'CAMPAIGN Facebook'
    and event_name_type = 'conversion'
    and toUnixTimestamp(at) >= 1606064400 and toUnixTimestamp(at) <= 1606582799
group by utm_content, utm_source;

----------------------num of order

select utm_content,
       count() as orders
from campaign_performance_data
where utm_campaign = 'CAMPAIGN Facebook'
    and event_name_type = 'conversion'
    and toUnixTimestamp(at) >= 1606064400 and toUnixTimestamp(at) <= 1606582799
group by utm_content
;


------------gop lai
select utm_content,
       sum(total_value) as revenue,
       count(distinct anonymous_id) as customers,
      count(*) as orders,
       revenue/customers as revenue_per_user,
       revenue/orders as avg_value_per_order

from campaign_performance_data
where tenant_id = 1
    and utm_campaign = 'CAMPAIGN Facebook'
    and event_name_type = 'conversion'
    and toUnixTimestamp(at) >= 1606064400 and toUnixTimestamp(at) <= 1606582799
group by utm_content
;



select utm_content,
       utm_source,
       gender,
       sum(total_value) as revenue,
       count(distinct anonymous_id) as customers,
      count(*) as orders,
       revenue/customers as revenue_per_user,
       revenue/orders as avg_value_per_order

from campaign_performance_data
where tenant_id = 1
    and utm_campaign = 'CAMPAIGN Facebook'
    and event_name_type = 'conversion'
    and toUnixTimestamp(at) >= 1606064400 and toUnixTimestamp(at) <= 1606582799
group by utm_content, utm_source, gender
;

-------------------------------------------channel click

SELECT utm_content,
      count(DISTINCT anonymous_id) AS clicks
FROM campaign_performance_data
WHERE tenant_id = 1
   AND  utm_campaign = 'CAMPAIGN Facebook'
   AND event_name = 'click'
   AND toUnixTimestamp(at) >= 1606064400 AND toUnixTimestamp(at) <= 1606582799
GROUP BY utm_content
;

--------------------------goal completed

SELECT utm_content,
      count(DISTINCT anonymous_id) AS goal_action_completion
FROM campaign_performance_data
WHERE tenant_id = 1
   AND  utm_campaign = 'CAMPAIGN Facebook'
   AND event_name_type = 'reach_goal'
   AND reach_goal_id = '1kiacEzd57VcHJwOcuzwwcFJx9Z'
   AND toUnixTimestamp(at) >= 1606064400 AND toUnixTimestamp(at) <= 1606582799
GROUP BY utm_content
;

select * from campaign_performance_data;
-----------------------------------




    SELECT utm_content,
           SUM(total_value) AS kpi_net_revenue
    FROM campaign_performance_data
    WHERE tenant_id = 1
        AND utm_campaign = 'CAMPAIGN Google'
        AND event_name_type = 'conversion'
        AND toUnixTimestamp(at) >= 0 AND toUnixTimestamp(at) <= 9999999999
        AND gender = 'Male'
    GROUP BY utm_content
    ORDER BY utm_content
;


select * from campaign_performance_data;

select toUnixTimestamp(toStartOfTenMinutes(at)) as time_stamp from campaign_performance_data;


alter table events_campaign delete where utm_campaign = 'CAMPAIGN Google' and utm_source = 'MailChimp';
;

select toStartOfTenMinutes()


    SELECT utm_content,utm_source,location,
            toUnixTimestamp(toStartOfTenMinutes(at)) as time_stamp,
           SUM(total_value) AS value
    FROM campaign_performance_data
    WHERE tenant_id = 1
        AND utm_campaign = 'CAMPAIGN Google'
        AND event_name_type = 'conversion'
        AND toUnixTimestamp(at) >= 1606443467 AND toUnixTimestamp(at) <= 1606582799
    GROUP BY utm_content,utm_source,location, time_stamp
    ORDER BY utm_content,utm_source,location, time_stamp

    ORDER BY         time_stamp
;



    SELECT utm_content,
           utm_source,
            toUnixTimestamp(toStartOfTenMinutes(at)),
           SUM(total_value)  as revenue
    FROM campaign_performance_data
    WHERE tenant_id = 1
        AND utm_campaign = 'CAMPAIGN Google'
        AND event_name_type = 'conversion'
        AND toUnixTimestamp(at) >= 0 AND toUnixTimestamp(at) <= 9999999999
    GROUP BY utm_content, utm_source, toUnixTimestamp(toStartOfTenMinutes(at))
    ORDER BY utm_content, utm_source, toUnixTimestamp(toStartOfTenMinutes(at))
;
;



    SELECT utm_content,
           SUM(total_value) AS value
    FROM campaign_performance_data
    WHERE tenant_id = 1
        AND utm_campaign = 'CAMPAIGN Google'
        AND event_name_type = 'conversion'
        AND toUnixTimestamp(at) >= 1606443467 AND toUnixTimestamp(at) <= 1606582799
    GROUP BY utm_content
;



    SELECT utm_content,
           utm_source,
           SUM(total_value) AS {metric_name}
    FROM campaign_performance_data
    WHERE tenant_id = {tenant_id}
        AND utm_campaign = '{utm_campaign}'
        AND event_name_type = 'conversion'
        AND toUnixTimestamp(at) >= {start_time} AND toUnixTimestamp(at) <= {end_time}
    GROUP BY {base_cols}, time_stamp
    ORDER BY {base_cols}, time_stamp


---------------------conversion rate


select orders,
       clicks
from (
         select utm_content,
                count() as orders
         from campaign_performance_data
         where utm_campaign = 'CAMPAIGN Facebook'
           and event_name_type = 'conversion'
           and toUnixTimestamp(at) >= 1606064400
           and toUnixTimestamp(at) <= 1606582799
         group by utm_content
         ) as u1
join
(
SELECT utm_content,
       toUnixTimestamp(toStartOfTenMinutes(at)) as time_stamp,
       countIf(event_name_type=='conversion')/ countIf(DISTINCT anonymous_id, event_name =='click') as rate
FROM campaign_performance_data
WHERE tenant_id = 1
   AND  utm_campaign = 'CAMPAIGN Facebook'
GROUP BY utm_content, time_stamp

) as u2 on u1.utm_content = u2.utm_content
;

-- AND toUnixTimestamp(at) >= 1606064400 AND toUnixTimestamp(at) <= 1606582799



--order
    select utm_content,
        toUnixTimestamp(toStartOfTenMinutes(at)) as time_stamp,
            count() as orders
     from campaign_performance_data
     where utm_campaign = 'CAMPAIGN Facebook'
       and event_name_type = 'conversion'
       and toUnixTimestamp(at) >= 1606064400
       and toUnixTimestamp(at) <= 1606582799
     group by utm_content, time_stamp
;

-- click
SELECT utm_content,
    toUnixTimestamp(toStartOfTenMinutes(at)) as time_stamp,
      count(DISTINCT anonymous_id) AS clicks
FROM campaign_performance_data
WHERE tenant_id = 1
   AND  utm_campaign = 'CAMPAIGN Facebook'
   AND event_name = 'click'
   AND toUnixTimestamp(at) >= 1606064400 AND toUnixTimestamp(at) <= 1606582799
GROUP BY utm_content, time_stamp;

select 41/114



1606214400,1
1606296600,2
1606215600,10


1606214400,3
1606296600,4
1606215600,30
1606299000,7
1606205400,6
1606294800,18
1606213800,5
1606206000,6
1606215000,6



    SELECT utm_content,utm_source,location,toUnixTimestamp(toStartOfTenMinutes(at)) AS time_stamp,
           SUM(total_value) AS value
    FROM campaign_performance_data
    WHERE tenant_id = 1
        AND utm_campaign = 'CAMPAIGN Google'
        AND event_name_type = 'conversion'
        AND toUnixTimestamp(at) >= 1606443467 AND toUnixTimestamp(at) <= 1606582799
    GROUP BY utm_content,utm_source,location,time_stamp
    ORDER BY value
    LIMIT 10;



        SELECT utm_content,utm_source,location,toUnixTimestamp(toStartOfTenMinutes(at)) AS time_stamp,
               countIf(event_name_type=='conversion') as orders,
               countIf(DISTINCT anonymous_id, event_name =='click') as clicks,
               (countIf(event_name_type=='conversion')/ countIf(DISTINCT anonymous_id, event_name =='click')) * 100 as value
        FROM campaign_performance_data
        WHERE tenant_id = 1
           AND  utm_campaign = 'CAMPAIGN Google'
        GROUP BY utm_content,utm_source,location,time_stamp;


select utm_content,
       utm_source,
       gender,
       device,
       location
from campaign_performance_data limit 10;


select utm_content,
       utm_source,
       groupArray(time_stamp),
       groupArray(clicks)
from (


select utm_content,
       utm_source,
       groupArray(time_stamp),
       groupArray(clicks)
from (
    SELECT utm_content,utm_source,
             toUnixTimestamp(toStartOfHour(at)) as time_stamp,
          count(DISTINCT anonymous_id) AS clicks
    FROM campaign_performance_data
    WHERE tenant_id = 1
       AND  utm_campaign = 'CAMPAIGN Facebook'
       AND event_name = 'click'
       AND toUnixTimestamp(at) >= 1606064400 AND toUnixTimestamp(at) <= 1606582799
    GROUP BY utm_content,utm_source, time_stamp
    ORDER BY utm_content, utm_source,time_stamp
)
group by utm_content, utm_source



)
;

  SELECT distinct (utm_content,utm_source, location)
    FROM campaign_performance_data
    WHERE tenant_id = 1
       AND  utm_campaign = 'CAMPAIGN Facebook'
       AND toUnixTimestamp(at) >= 1606064400 AND toUnixTimestamp(at) <= 1606582799;





group by utm_content, utm_source


select FROM_UNIXTIME()
 WITH FILL FROM toStartOfHour(FROM_UNIXTIME(1606064400)) TO toStartOfHour(FROM_UNIXTIME(1606582799)) STEP 3600;


    SELECT utm_content,utm_source,location,toUnixTimestamp(toStartOfTenMinutes(at)) AS time_stamp,
           SUM(total_value) AS value
    FROM campaign_performance_data
    WHERE tenant_id = 1
        AND utm_campaign = 'CAMPAIGN Google'
        AND event_name_type = 'conversion'
        AND toUnixTimestamp(at) >= 1606443467 AND toUnixTimestamp(at) <= 1606582799
    GROUP BY utm_content,utm_source,location,time_stamp
;

select toUnixTimestamp(toDateTime('2020-12-01 11:11:11')); --1606821071
select toUnixTimestamp(toDateTime('2020-12-10 10:05:20')); --1607594720


select toStartOfDay(FROM_UNIXTIME(1606821071));




select time_stamp
order by time_stamp with fill from toStartOfDay(FROM_UNIXTIME(start)) to  toStartOfDay(FROM_UNIXTIME(end))

;

select toStartOfDay();

select * from campaign_performance_data where utm_campaign = 'CAMPAIGN Google';


drop table test;
create table test(
    d UInt64
)ENGINE =Memory()
;
insert into test values (1234567890);

select d
from test
order by d with fill from toUnixTimestamp( toStartOfDay(FROM_UNIXTIME(1606821071))) to toUnixTimestamp( toStartOfDay(FROM_UNIXTIME(1607594720))) STEP 24*60*60;

select toUnixTimestamp();


show tables;
drop table test;


show create table campaign_performance_data;


CREATE VIEW campaign_performance_data
AS
SELECT
    tenant_id,
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
FROM
(
    SELECT
        tenant_id,
        anonymous_id,
        event_name,
        event_name_type,
        utm_campaign,
        utm_content,
        utm_source,
        `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] AS total_value,
        `str_properties.vals`[indexOf(`str_properties.keys`, 'reach_goal_id')] AS reach_goal_id,
        at
    FROM events_campaign
) AS ec
INNER JOIN
(
    SELECT
        tenant_id,
        anonymous_id,
        str_vals[indexOf(str_keys, 'gender')] AS gender,
        str_vals[indexOf(str_keys, 'city')] AS location,
        str_vals[indexOf(str_keys, 'device_platform')] AS device
    FROM
    (
        SELECT
            tenant_id,
            anonymous_id,
            groupArray(str_key) AS str_keys,
            groupArray(str_val) AS str_vals
        FROM profile_str_final_v
        WHERE str_key IN ('gender', 'city', 'device_platform')
        GROUP BY
            tenant_id,
            anonymous_id
    )
) AS pf ON (ec.tenant_id = pf.tenant_id) AND (ec.anonymous_id = pf.anonymous_id)
ORDER BY
    tenant_id ASC,
    utm_campaign ASC,
    event_name_type ASC,
    at ASC
;


select * from events_campaign order by at desc ;
select * from campaign_performance_data order by at desc ;








ent_name,
       event_name_type,
       utm_campaign,
       utm_content,
       utm_source,
       total_value,
       time_stamp,
       gender,
       location,
       device
FROM
(
    SELECT
        tenant_id,
        anonymous_id,
        event_name,
        event_name_type,
        utm_campaign,
        utm_content,
        utm_source,
        `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] AS total_value,
        toUnixTimestamp(toStartOfTenMinutes(at)) AS time_stamp
    FROM events_campaign
    WHERE tenant_id = 1
      AND utm_campaign = %(utm_campaign)s
      AND toUnixTimestamp(at) >= %(start_time)s
      AND toUnixTimestamp(at) <= 1606928399
) AS ec
INNER JOIN
(
    SELECT
           tenant_id,
           anonymous_id,
           str_vals[indexOf(str_keys, 'gender')] AS gender,
           str_vals[indexOf(str_keys, 'city')] AS location,
           str_vals[indexOf(str_keys, 'device_platform')] AS device
    FROM
    (
        SELECT tenant_id,
               anonymous_id,
               groupArray(str_key) AS str_keys,
               groupArray(str_val) AS str_vals
        FROM profile_str_final_v
        WHERE tenant_id = 1
              AND  anonymous_id IN (SELECT anonymous_id
                                    FROM events_campaign
                                    WHERE tenant_id = 1
                                          AND utm_campaign = 'CAMPAIGN Google'
                                          AND toUnixTimestamp(at) >= 1606755600
                                          AND toUnixTimestamp(at) <= 1606928399 )
--               AND str_key IN ('gender', 'city', 'device_platform')
         GROUP BY tenant_id, anonymous_id
    )
) AS pf
ON ec.tenant_id = pf.tenant_id AND ec.anonymous_id = pf.anonymous_id


2020-12-02 10:02:49,122 INFO sqlalchemy.engine.base.Engine {'tenant_id': 1, 'utm_campaign': 'CAMPAIGN Google', 'reach_goal_id': '1kilgrP7I6dnkdd9D5UmptGHZpM', 'start_time': 1606755600, 'end_time': 1606928399}

-------check profile

select FROM_UNIXTIME(1606755600) as start, FROM_UNIXTIME(1606928399) as end;

select * from profile_str_final_v order by at desc ;


--------------------------------------------


CREATE VIEW campaign_performance_data_v
AS
SELECT
    tenant_id,
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
FROM
(
    SELECT
        tenant_id,
        anonymous_id,
        event_name,
        event_name_type,
        utm_campaign,
        utm_content,
        utm_source,
        `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] AS total_value,
        `str_properties.vals`[indexOf(`str_properties.keys`, 'reach_goal_id')] AS reach_goal_id,
        at
    FROM events_campaign
) AS ec
INNER JOIN
(
    SELECT
        tenant_id,
        anonymous_id,
        str_vals[indexOf(str_keys, 'gender')] AS gender,
        str_vals[indexOf(str_keys, 'city')] AS location,
        str_vals[indexOf(str_keys, 'device_platform')] AS device
    FROM
    (
        SELECT
            tenant_id,
            anonymous_id,
            groupArray(str_key) AS str_keys,
            groupArray(str_val) AS str_vals
        FROM profile_str_final_v
--         WHERE str_key IN ('gender', 'city', 'device_platform')
        GROUP BY
            tenant_id,
            anonymous_id
    )
) AS pf ON (ec.tenant_id = pf.tenant_id) AND (ec.anonymous_id = pf.anonymous_id)
ORDER BY
    tenant_id ASC,
    utm_campaign ASC,
    event_name_type ASC,
    at ASC
;

select count(*) from campaign_performance_data;
select count(*) from campaign_data_joined_v;