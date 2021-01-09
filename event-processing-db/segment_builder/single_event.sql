select *
from events;
select distinct event_name
from events;

----------------------------------------SINGLE EVENT

-- without filter
select count() as audience_size
from (
      select count() as no_event
      from events
      where event_name = 'cart_viewed_(r)'
      group by tenant_id, anonymous_id)
where no_event > 1
;


-- filter
--
select *
from events
where event_name = 'cart_viewed_(r)'
--   and `str_properties.vals`[indexOf(`str_properties.keys`, 'target.itemId')] == '/event-traffic'; -- equal
-- and `str_properties.vals`[indexOf(`str_properties.keys`, 'target.itemId')] == ''; -- exist: number not exist 0, string: ''
--     and endsWith(`str_properties.vals`[indexOf(`str_properties.keys`, 'target.itemId')], 'ffic' ) -- endsWith
--     and startsWith(`str_properties.vals`[indexOf(`str_properties.keys`, 'target.itemId')], '/event' ) -- endsWith
--    and positionCaseInsensitive(`str_properties.vals`[indexOf(`str_properties.keys`, 'target.itemId')], 'event') == 0 -- contain and not contain
  -- boolean
  and `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')]  > 0
  and at >= subtractDays(now(), 30)  and at <= now()
;

-- and/ or

select *
from events
where (event_name = 'cart_viewed_(r)'
and `str_properties.vals`[indexOf(`str_properties.keys`, 'target.itemId')] == '/event-traffic')
and (event_name = 'checkout_completed_(r))'
and `str_properties.vals`[indexOf(`str_properties.keys`, 'target.itemId')] != 'aaa') ;

------------------------------------ TRAIT
select count(distinct tenant_id, anonymous_id)
from (
      select tenant_id, anonymous_id
      from (
            select tenant_id, anonymous_id
            from profile_str_final_v
            where str_key == 'last_name'
              and str_val == 'Do'
               ) as t1
      --union all -- or,
      inner join -- and
      (
          select tenant_id, anonymous_id
          from profile_num_final_v
          where num_key == 'recency'
            and num_val == '0'
      ) as t2
     on t1.tenant_id = t2.tenant_id and t1.anonymous_id = t2.anonymous_id

)
;


------------------------------------ PTRAIT


-------------------------------------FUNNEL
select * from events order by tenant_id, anonymous_id;
select * from events where tenant_id = 0 and anonymous_id ='DFUd9C2UUOF2pObZDCQyZQi76zI' and event_name in ['identify', 'checkout_completed'] order by at ;


-- identify > 1 and checkout_complete > 1
select * from (
    SELECT tenant_id, anonymous_id,
        sequenceCount('(?1)(?1).*(?2)(?2).*')(at, event_name='identify', event_name='checkout_completed') AS count
    FROM events
    GROUP BY tenant_id, anonymous_id
) where count > 0
;

-- identify > 1 and checkout_complete >= 1


-- identify < 3 and checkout_complete < 3
select * from (
    SELECT tenant_id, anonymous_id,
        sequenceCount('(?1).*(?2)')(at, event_name='identify', event_name='checkout_completed') AS total1,
        sequenceCount('(?1)(?1)(?1).*(?2)(?2)(?2).*')(at, event_name='identify', event_name='checkout_completed') AS c1,
        total - c as count
    FROM events
    GROUP BY tenant_id, anonymous_id

) where count > 0
;




-- identify < 3 and pageview >=3
select * from (
               select tenant_id,
                      anonymous_id,
                      sequenceCount('(?1).*(?2)(?2)(?2).*')(at, event_name = 'identify', event_name = 'checkout_completed') as total,
                      sequenceCount('(?1)(?1)(?1).*(?2)(?2)(?2).*')(at, event_name = 'identify', event_name = 'checkout_completed') as leftovers,
                      total - leftovers                      as count
               from events
               group by tenant_id, anonymous_id
)
where count > 0
;


















------
select * from profile_str_final_v where anonymous_id ='DETssvY2oYCK9ANTaTpLqmDGrnb' and str_key = 'last_name';


select *
from profile_str_final_v
where str_key in ['last_name', 'dob']
group by tenant_id, anonymous_id
having count() = 2
;


select * from (
select tenant_id, anonymous_id, arrayZip(groupArray(str_key),groupArray(str_val)) as pros
from profile_str_final_v
group by tenant_id, anonymous_id
              )
where ('last_name', 'Doan') in pros and ('dob', '04/03/1988') in pros
;



select *
