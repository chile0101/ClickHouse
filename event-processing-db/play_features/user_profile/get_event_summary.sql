
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



----------------------------------------TEST AFTER HAVE DATA SAMPLE
select * from events;



SELECT
    tenant_id,
    anonymous_id,
    event_name,
    count(event_name) AS count,
    min(at) AS first_time,
    max(at) AS last_time
FROM events
WHERE (tenant_id =1) AND (anonymous_id ='DBzEQzzUKZVe19AhAyKrHG0SpOe')
GROUP BY
    tenant_id,
    anonymous_id,
    event_name
ORDER BY event_name ASC
LIMIT 0 * 10, 10

--------------------------user activity

select * from events;


SELECT
    id,
    tenant_id,
    anonymous_id,
    toUnixTimestamp(toStartOfDay(at)) AS date_unix,
    toUnixTimestamp(at) AS time_unix,
    session_id,
    event_name,
    arrayZip(`identity.keys`, `identity.vals`) AS identities,
    arrayZip(`str_properties.keys`, `str_properties.vals`) AS str_pros,
    arrayZip(`num_properties.keys`, `num_properties.vals`) AS num_pros,
    arrayZip(`arr_properties.keys`, `arr_properties.vals`)AS arr_pros
FROM events
WHERE (tenant_id =1)
    AND (anonymous_id = 'DBzEQzzUKZVe19AhAyKrHG0SpOe')
    AND (event_name IN ['sessionCreated','sessionUpdated','view'])
ORDER BY at DESC
LIMIT 0 * 10, 10
;


select count() from events;

select * from events where anonymous_id = 'DBWTCJnlKs3bEARMRaK1r9C9Cim';


select * from user_profile_final where anonymous_id = 'DBWTCJnlKs3bEARMRaK1r9C9Cim';

 SELECT
            tenant_id,
            anonymous_id,
            event_name,
            count(event_name) AS count,
            min(at) AS first_time,
            max(at) AS last_time
        FROM events
        WHERE (tenant_id = 1) AND (anonymous_id = 'DBWTCJnlKs3bEARMRaK1r9C9Cim')
        GROUP BY
            tenant_id,
            anonymous_id,
            event_name
        ORDER BY event_name ASC
        LIMIT 0, 10
