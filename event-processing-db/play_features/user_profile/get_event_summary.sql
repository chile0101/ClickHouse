
show tables;

select tenant_id,
       anonymous_id,
       event_name,
       generateUUIDv4() as uuid_row,
       count(event_name) as count,
       min(at) as first_time,
       max(at) as last_time
from events
where tenant_id = 1 and anonymous_id = 'a1'
group by tenant_id, anonymous_id, event_name
order by event_name
;


select count(*)
from events
where tenant_id = 1 and anonymous_id = 'a1'
group by tenant_id, anonymous_id, event_name
;

select count(distinct event_name) as total_rows
from events
where tenant_id = 1 and anonymous_id = 'a1'
;


SELECT
    length(argMaxMerge(users)) AS segment_size
FROM segment_users_final
WHERE tenant_id = 1 and segment_id = 's1';



SELECT
    tenant_id,
    anonymous_id,
    event_name,
    count(event_name) AS count,
    min(at) AS first_time,
    max(at) AS last_time
FROM events
WHERE (tenant_id = 1) AND (anonymous_id = 'a1')
GROUP BY
    tenant_id,
    anonymous_id,
    event_name
ORDER BY event_name ASC
;