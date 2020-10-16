

show tables;

WITH (
    select count(*) from events where (tenant_id = 1)
  AND (anonymous_id = 'a1')
  AND (event_name IN ['App Launched', 'App Uninstalled', 'Category Viewed', 'Searched'])
    ) as count
SELECT
    count,
    tenant_id,
    anonymous_id,
    toUnixTimestamp(toStartOfDay(at)) AS date,
    toUnixTimestamp(at) AS time,
    `str_properties.vals`[indexOf(`str_properties.keys`, 'session_id')] as session_id,
    event_name,
    arrayZip(`identity.keys`, `identity.vals`) as identities,
   arrayFilter(x -> x.1 in ['product','device'] ,arrayZip(`str_properties.keys`, `str_properties.vals`)) as str_pros,
   arrayFilter(x -> x.1 in [] ,arrayZip(`num_properties.keys`, `num_properties.vals`)) as num_pros,
   arrayFilter(x -> x.1 in [] ,arrayZip(`arr_properties.keys`, `arr_properties.vals`)) as arr_pros
FROM events
WHERE (tenant_id = 1)
  AND (anonymous_id = 'a1')
  AND (event_name IN ['App Launched', 'App Uninstalled', 'Category Viewed', 'Searched'])
ORDER BY at DESC
LIMIT 0 * 10, 10
;


select * from events;

select toUnixTimestamp(toDateTime('2020-10-09 00:00:00'));
select toUnixTimestamp(toDateTime('2020-10-09 03:20:28'));


select toUnixTimestamp(toDateTime('2020-10-09 03:20:28')) AS time;
select toUnixTimestamp(toStartOfDay(toDateTime('2020-09-10 00:00:00'))) AS date;
select toStartOfDay(toDateTime('2020-09-10 00:00:00'));


SELECT arrayFilter(x -> x LIKE '%World%', ['Hello', 'abc World']) AS res;

select arrayFilter(x -> x in ['product','device'], `str_properties.keys`) as keys,
       arrayFilter((x,y) ->  , `str_properties.keys`, `str_properties.vals`) as vals
from events;

--arrayFilter((x, y) -> ((x LIKE 'iphone%') AND (y >= 5)), products.phones, products.time) AS phone
-- array zip ?

select
      arrayFilter(x ->[x.1, x.2] if x.1 in ['product','device'] ,arrayZip(`str_properties.keys`, `str_properties.vals`)) as str_pros
from events;


select arrayZip(`str_properties.keys`, `str_properties.vals`)[1].1
from events;



SELECT
    tenant_id,
    anonymous_id,
    toUnixTimestamp(toStartOfDay(at)) AS date_unix,
    toUnixTimestamp(at) AS time_unix,
    `str_properties.vals`[indexOf(`str_properties.keys`, 'session_id')] AS session_id,
    event_name,
    arrayZip(`identity.keys`, `identity.vals`) AS identities,
    arrayFilter(x -> ((x.1) IN :str_pros_filter), arrayZip(`str_properties.keys`, `str_properties.vals`)) AS str_pros,
    arrayFilter(x -> ((x.1) IN :num_pros_filter), arrayZip(`num_properties.keys`, `num_properties.vals`)) AS num_pros,
    arrayFilter(x -> ((x.1) IN :arr_pros_filter), arrayZip(`arr_properties.keys`, `arr_properties.vals`)) AS arr_pros
FROM events
WHERE (tenant_id = :tenant_id)
    AND (anonymous_id = :anonymous_id)
    AND (event_name IN :event_names)
ORDER BY at DESC
LIMIT :page_number * :page_size, :page_size
;





SELECT
    event_id,
    tenant_id,
    anonymous_id,
    toUnixTimestamp(toStartOfDay(at)) AS date_unix,
    toUnixTimestamp(at) AS time_unix,
    `str_properties.vals`[indexOf(`str_properties.keys`, '{SESSION_ID}')] AS session_id,
    event_name,
    arrayZip(`identity.keys`, `identity.vals`) AS identities,
    arrayFilter(x -> ((x.1) IN :str_pros_filter), arrayZip(`str_properties.keys`, `str_properties.vals`)) AS str_pros,
    arrayFilter(x -> ((x.1) IN :num_pros_filter), arrayZip(`num_properties.keys`, `num_properties.vals`)) AS num_pros,
    arrayFilter(x -> ((x.1) IN :arr_pros_filter), arrayZip(`arr_properties.keys`, `arr_properties.vals`)) AS arr_pros

FROM events
WHERE (tenant_id = :tenant_id)
    AND (anonymous_id = :anonymous_id)
ORDER BY at DESC