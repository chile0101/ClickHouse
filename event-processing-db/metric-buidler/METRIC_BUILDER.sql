





;
select * from events;
select distinct event_name from events;
select max(`num_properties.vals`[indexOf(`num_properties.keys`, 'source.properties.age')])
from events
where event_name = ''
;
select sum(`num_properties.vals`[indexOf(`num_properties.keys`, 'source.properties.age')])
from events
where event_name = ''
;

-- COUNT
select count()
from events
where event_name = 'view'
;

select count()
from events
where event_name = 'checkout_completed'
and `str_properties.vals`[indexOf(`str_properties.keys`, 'properties.currency')] = 'VND'
;

select count()
from events
where event_name = 'checkout_completed'
and `str_properties.vals`[indexOf(`str_properties.keys`, 'properties.currency')] not in ['asdf', 'USD']
;

select count()
from events
where event_name = 'checkout_completed'
and positionCaseInsensitive(`str_properties.vals`[indexOf(`str_properties.keys`, 'properties.currency')], 'VN') = 1
;

select count()
from events
where event_name = 'checkout_completed'
and match(`str_properties.vals`[indexOf(`str_properties.keys`, 'properties.currency')], 'N')
;

select count()
from events
where event_name = 'checkout_completed'
and  not positionCaseInsensitive(`str_properties.vals`[indexOf(`str_properties.keys`, 'properties.currency')], 'VNDD')

;

select not positionCaseInsensitive(`str_properties.vals`[indexOf(`str_properties.keys`, 'properties.currency')], 'VND')
from events
where event_name = 'checkout_completed'
and  not positionCaseInsensitive(`str_properties.vals`[indexOf(`str_properties.keys`, 'properties.currency')], 'VNDD')



select count()
from events
where event_name = 'checkout_completed'
and `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] between 492088 and 492090
;

            select count()
            from events
            where event_name = 'checkout_completed'
            and less(`num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')], 492090)

-- AVG
select sum(`num_properties.vals`[indexOf(`num_properties.keys`, 'source.properties.age')]) as sum,
       count() as count,
       sum/count as avg,
       avg(`num_properties.vals`[indexOf(`num_properties.keys`, 'source.properties.age')]) as avg_2
from events
;


-- fist last
-- num or str
select `num_properties.vals`[indexOf(`num_properties.keys`, 'source.properties.age')] as age
from events
order by at
limit 1
;
-- exist
select toInt8(count() > 0)
from events
where event_name = 'view'
;
-- most common
-- age xuat hien nhieu nhat
-- num or str
select `num_properties.vals`[indexOf(`num_properties.keys`, 'source.properties.age')] as value
from events
group by value
order by count(value) desc
limit 1
;
-- count distinct
-- bao nhieu gia tri tuoi khac nhau
with `num_properties.vals`[indexOf(`num_properties.keys`, 'source.properties.age')] as age
select count(distinct age)
from events
;
drop table metric_results;
create table metric_results(
    tenant_id UInt16,
    date Date,
    str_values Array(String),
    num_values Array(Float64),
    metric_definition_id UInt16,
    created_at DateTime
)ENGINE MergeTree()
PARTITION BY toYYYYMM(created_at)
order by (tenant_id, date, created_at)
;
-- insert
insert into metric_results values
-- (1, '2021-01-01', [], [1,2,3], 1, now()),
-- (1, '2021-01-02', [], [2,3,4], 1, now())
(1, '2021-01-02', [], [4,3,6], 1, now())
(1, '2021-01-02', [], [10], 1, now())
;
select * from metric_results;
-- distinct value
with `num_properties.vals`[indexOf(`num_properties.keys`, 'source.properties.age')] as age
select distinct age
from events


select groupArray(distinct (num_values))
from metric_results array join num_values
;
-- count distinct
select count(distinct num_values)
from metric_results array join num_values
--


---------------------------------
select distinct event_name from events;
select count() from events;
insert into eventify_stag.events
select * from eventify.events where event_name = 'checkout_completed';

select *
from events
where event_name = 'checkout_completed';

['source.itemId','source.itemType','source.properties.connection_type','properties.currency','campaign_content','campaign_name','campaign_tactic','campaign_source','device_model','device_name','device_brand','device_type','ip','os_name','connection_type']['https://nih.gov/quis.aspxt','page','https://nih.gov/quis.aspxt','category','High Risk of Churn Activation','Facebook','4g','New Arrival Tactic','cpc','edu','Lamb - Whole, Frozen','https://nih.gov/quis.aspxt','New Arrival Tactic','High Risk of Churn Activation','cpc','Facebook','Desktop','Apple Macintosh','Apple','Desktop','113.176.61.79','Mac OS X','4g']
['http://delicious.com/blandit/ultrices/enim/lorem.xml?adipiscing=odio&lorem=in&vitae=hac&mattis=habitasse&nibh=platea&ligula=dictumst&nec=maecenas&sem=ut&duis=massa','page','4g','VND','New Arrival Tactic','High Risk of Churn Activation','cpa','Facebook','Desktop','Apple Macintosh','Apple','Desktop','14.161.38.118','Mac OS X','4g']

['source.properties.screen_height','source.properties.screen_width','properties.total_value','properties.schema_invalid']
[150,2048,492088,1]
;

select sum(`num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')])
from events
where event_name = 'checkout_completed'
and equals(`str_properties.vals`[indexOf(`str_properties.keys`, 'properties.currency')], 'VND');



            select `num_properties.vals`[indexOf(`num_properties.keys`, 'source.properties.age')] as value
            from events
            where event_name = 'checkout_completed'
                and equals(`str_properties.vals`[indexOf(`str_properties.keys`, 'properties.currency')], 'VND')
            group by value
            order by count(value) desc
            limit 1
;

            select distinct `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] as value
            from events
            where event_name = 'checkout_completed'
                and equals(`str_properties.vals`[indexOf(`str_properties.keys`, 'properties.currency')], 'VND')


--------------------------------------------- TRAIT
select * from events;
select * from profile_str;

select count() as value
from events
where tenant_id = 1
and event_name = 'checkout_completed'
;

select anonymous_id
from events
where tenant_id = 1
and event_name = 'checkout_completed'


-- 33246 1821
select count(distinct anonymous_id)
from profile_str_final_v
where tenant_id = 1
      and str_key in ('gender', 'country') and str_val in ('Female', 'Vietnam')
;



--------------------- AND PTRAIT
( select t0.tenant_id as tenant_id, t0.anonymous_id as anonymous_id
from
    (select tenant_id, anonymous_id
    from profile_str_final_v
    where tenant_id = 1
    and str_key = 'gender'
    and positionCaseInsensitive(str_val, 'male') > 0)
as t0
 inner join
    (select tenant_id, anonymous_id
    from profile_num_final_v
    where tenant_id = 1
    and num_key = 'age'
    and num_val > 1)
as t1

 on t0.tenant_id=t1.tenant_id and t0.anonymous_id=t1.anonymous_id )


-----------------------OR PTRAIT
( select tenant_id, anonymous_id
from (
    (select tenant_id, anonymous_id
    from profile_str_final_v
    where tenant_id = 1
    and str_key = 'gender'
    and positionCaseInsensitive(str_val, 'male') > 0)
union all
    (select tenant_id, anonymous_id
    from profile_num_final_v
    where tenant_id = 1
    and num_key = 'age'
    and num_val > 1)
) )
;


-- BUILD step by step
select * from profile_str_final_v where tenant_id = 1;
select * from profile_num_final_v where tenant_id = 1;
-- 1 node
select anonymous_id
    from profile_num_final_v
    where tenant_id = 1
    and (num_key = 'age' and num_val > 1)


-- str and str
select anonymous_id -- 210
from
(
    select anonymous_id --975
    from profile_str_final_v
    where tenant_id = 1
      and (str_key = 'gender' and str_val = 'Female')
) as p1
inner join
(
     select anonymous_id -- 796
    from profile_str_final_v
    where tenant_id = 1
    and (str_key = 'rfm' and str_val = 'Silver')
) as p2
on p1.anonymous_id = p2.anonymous_id

-- str or str
select distinct anonymous_id -- 210
from
(
    select anonymous_id --975
    from profile_str_final_v
    where tenant_id = 1
      and (str_key = 'gender' and str_val = 'Female')
)
UNION ALL
(
     select anonymous_id -- 796
    from profile_str_final_v
    where tenant_id = 1
    and (str_key = 'rfm' and str_val = 'Silver')
)
;


-------- merge profiles
select anonymous_id
    from profile_str_final_v
    where tenant_id = 1
    and (str_key = 'age' and num_val > 1)
;
            select *
            from profile_str_final_v
            where tenant_id = 1
--                 and (str_key = 'gender' and str_val = 'Female')
--                 and (str_key = 'rfm' and str_val = 'Silver');
                    and str_key in ['gender','rfm'] and str_val in ['Female', 'Silver'];




            select anonymous_id
            from profile_str_final_v
            where tenant_id = 1 and str_key = 'gender' and str_val = 'Female';


select *
from (
    select
           anonymous_id,
           groupArray(str_key) as str_keys,
           groupArray(str_val) as str_vals,
           groupArray(num_key) as num_keys,
           groupArray(num_val) as num_vals
    from
      (
          select *
          from profile_str_final_v
          where tenant_id = 1 -- and str_key in ['gender'] and str_val in ['Female']

      ) as ps
    inner join
        (
            select *
            from profile_num_final_v
            where tenant_id = 1  --and num_key in [] and num_val in []
        ) as pn
    on (ps.tenant_id = pn.tenant_id and ps.anonymous_id = pn.anonymous_id)
    group by tenant_id, anonymous_id
)
where str_vals[indexOf(str_keys, 'gender')] = 'Female'
and
   (num_vals[indexOf(num_keys, 'age')] >= 0
    or str_keys[indexOf(str_keys, 'acquisition_type')] = 'facebook'
  )
;

--

----------------str and str
select
   *
from
(
  select anonymous_id,
        groupArray(str_key) as str_keys,
        groupArray(str_val) as str_vals
  from profile_str_final_v
  where tenant_id = 1 -- and str_key in ['gender','rfm'] and str_val in ['Female', 'Silver']
  group by tenant_id, anonymous_id
)
where str_vals[indexOf(str_keys, 'gender')] = 'Female'
  and (str_vals[indexOf(str_keys, 'rfm')]) = 'Silver'
; -- 210 -- 111 ms

-- wrong
  select anonymous_id
  from profile_str_final_v
  where tenant_id = 1 and str_key in ['gender','rfm'] and str_val in ['Female', 'Silver']
  group by tenant_id, anonymous_id
  having count() = 2 -- 210 96 ms
;
  -- str or str
select *
from
(
  select anonymous_id,
        groupArray(str_key) as str_keys,
        groupArray(str_val) as str_vals
  from profile_str_final_v
  where tenant_id = 1 and str_key in ['gender','rfm'] and str_val in ['Female', 'Silver']
  group by tenant_id, anonymous_id
)
where str_vals[indexOf(str_keys, 'gender')] = 'Female'
  or (str_vals[indexOf(str_keys, 'rfm')]) = 'Silver' -- 1561
;
  select distinct anonymous_id
from profile_str_final_v
where tenant_id = 1
    and ((str_key = 'gender' and str_val = 'Female') or (str_key = 'rfm' and str_val = 'Silver')); -- 1561

-- STR and NUM

select anonymous_id
from (
    select *
--            anonymous_id,
--            groupArray(str_key) as str_keys,
--            groupArray(str_val) as str_vals,
--            groupArray(num_key) as num_keys,
--            groupArray(num_val) as num_vals
    from
      (
          select anonymous_id, str_key, str_val
          from profile_str_final_v
          where tenant_id = 1  and str_key in ['gender'] and str_val in ['Female']
      ) as ps
    inner join
        (
            select anonymous_id, num_key, num_val
            from profile_num_final_v
            where tenant_id = 1  and num_key in ['age'] and num_val in [61]
        ) as pn
    on ps.anonymous_id = pn.anonymous_id
    group by anonymous_id
)
where str_vals[indexOf(str_keys, 'gender')] = 'Female'
    and num_vals[indexOf(num_keys, 'age')] = 61
;

insert into profile_str values ('DF04kyX04Tp9CTIN3h3rT0XLbm9',1, 'gender', 'Male', now64());
insert into profile_num values ('DF04kyX04Tp9CTIN3h3rT0XLbm9',1 , 'age', 61, now64());





            select count(anonymous_id)
            from (
                select
                       anonymous_id,
                       groupArray(str_key) as str_keys,
                       groupArray(str_val) as str_vals,
                       groupArray(num_key) as num_keys,
                       groupArray(num_val) as num_vals
                from
                  (
                      select anonymous_id, str_key, str_val
                      from profile_str_final_v
                      where tenant_id = 1  and str_key in ['gender'] and str_val in ['Female']
                  ) as ps
                inner join
                    (
                        select anonymous_id, num_key, num_val
                        from profile_num_final_v
                        where tenant_id = 1  and num_key in ['age'] and num_val in [61]
                    ) as pn
                on ps.anonymous_id = pn.anonymous_id
                group by anonymous_id
            )
            where str_vals[indexOf(str_keys, 'gender')] = 'Female'
                and num_vals[indexOf(num_keys, 'age')] = 61 -- 14


                select
                       anonymous_id
                from
                  (
                      select anonymous_id
                      from profile_str_final_v
                      where tenant_id = 1  and str_key = 'gender' and str_val in 'Female'
                  ) as ps
                inner join
                    (
                        select anonymous_id
                        from profile_num_final_v
                        where tenant_id = 1  and num_key = 'age' and num_val = 61
                    ) as pn
                on ps.anonymous_id = pn.anonymous_id;

-- STR or NUM

select
      distinct anonymous_id
from (
        (
          select anonymous_id
          from profile_str_final_v
          where tenant_id = 1
            and str_key = 'gender'
            and str_val = 'Female' --974
        )
        union all
        (
          select anonymous_id
          from profile_num_final_v
          where tenant_id = 1
            and num_key = 'age'
            and num_val = 61 -- 34
        ) -- 1008
 )
--     on p1.anonymous_id = p2.anonymous_id
;
select 974 + 34;
select 1008 - 14;

-- STR or NUM

select count(anonymous_id)
from (
    select
           anonymous_id
--            groupArray(str_key) as str_keys,
--            groupArray(str_val) as str_vals,
--            groupArray(num_key) as num_keys,
--            groupArray(num_val) as num_vals,
--            count()
    from
      (
          select anonymous_id, str_key, str_val
          from profile_str_final_v
          where tenant_id = 1  and str_key in ['gender'] and str_val in ['Female'] --974
      ) as ps
    full join
        (
            select anonymous_id, num_key, num_val
            from profile_num_final_v
            where tenant_id = 1  and num_key in ['age'] and num_val in [61] -- 34
        ) as pn
    on ps.anonymous_id = pn.anonymous_id
    group by anonymous_id
)
where str_vals[indexOf(str_keys, 'gender')] = 'Female'
    or num_vals[indexOf(num_keys, 'age')] = 61 -- 14

---------------------------
            select
                  distinct anonymous_id
            from (
                    (
                      select anonymous_id
                      from profile_str_final_v
                      where tenant_id = 1
                        and str_key = 'gender' and str_val = 'Female' --974
                    )
                    union all
                    (
                      select anonymous_id
                      from profile_num_final_v
                      where tenant_id = 1
                        and num_key = 'age' and num_val = 61 -- 34
                    ) -- 1008
             )
;


select *
from (
    select
           if(ps.anonymous_id != '', ps.anonymous_id, pn.anonymous_id) as anonymous_id,
           str_keys, str_vals, num_keys, num_vals
    from
            (
                  select anonymous_id,
                         groupArray(str_key) as str_keys,
                         groupArray(str_val) as str_vals
                  from profile_str_final_v
                  where tenant_id = 1  and str_key in ['gender'] and str_val in ['g'] --974
                  group by anonymous_id
            ) as ps
        full join
            (
                select anonymous_id,
                       groupArray(num_key) as num_keys,
                       groupArray(num_val) as num_vals
                from profile_num_final_v
                where tenant_id = 1  and num_key in ['age', 'revenue'] and num_val in [1, 1] -- 191
                group by anonymous_id
            ) as pn
        on ps.anonymous_id = pn.anonymous_id -- 1070
)
where str_vals[indexOf(str_keys, 'gender')] = 'g'
 and (num_vals[indexOf(num_keys, 'age')] = 1 or num_vals[indexOf(num_keys, 'revenue')] = 1 )
;


-- inner 97
select 974 + 191; -- 1165
select 1165 - 97; -- 1068

insert into profile_str values ('t1', 1, 'gender', 'g', now64());
insert into profile_num values ('t1', 1, 'revenue', 1, now64());
insert into profile_num values ('t1', 1, 'age', 1, now64());

insert into profile_str values ('t2', 1, 'gender', 'g', now64());
insert into profile_num values ('t2', 1, 'revenue', 2, now64());
insert into profile_num values ('t2', 1, 'age', 2, now64());

insert into profile_str values ('t3', 1, 'gender', 'g1', now64());
insert into profile_num values ('t3', 1, 'revenue', 1, now64());
insert into profile_num values ('t3', 1, 'age', 1, now64());

insert into profile_str values ('t4', 1, 'gender', 'g', now64());
insert into profile_num values ('t4', 1, 'revenue', 1, now64());
insert into profile_num values ('t4', 1, 'age', 2, now64());

select * from eventify.events;