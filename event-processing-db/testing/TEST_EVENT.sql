select  *
from events
where event_name = 'checkout_completed'
limit 30;






SELECT u.tenant_id as tenant_id,
       u.anonymous_id as anonymous_id,
       e.total_value as total_value,
       e.count as count,
       u.date as date
FROM (
         WITH
             `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] AS value
         SELECT tenant_id,
                anonymous_id,
                sum(value) AS total_value,
                count() AS count,
                toDate(at) AS date
         FROM events
         WHERE event_name = 'checkout_completed'
           AND at >= '2020-10-28 00:00:00'
           AND at < '2020-10-29 00:00:00'
         GROUP BY tenant_id, anonymous_id, date
) AS e
RIGHT JOIN
(
    SELECT tenant_id,
           anonymous_id,
           toDate(at_final) AS date
    FROM user_profile_final
) AS u
ON e.tenant_id = u.tenant_id AND e.anonymous_id = u.anonymous_id
WHERE u.date < '2020-10-29'
;





