select *
from events_campaign;




WITH
    24*60*60 AS delta,
    1601510400 AS start_unix,
    1601514000 AS end_unix,
    ceil((end_unix - start_unix) / delta) + 1 AS n
SELECT toStartOfDay(FROM_UNIXTIME(start_unix)) + number * delta AS date_time
FROM system.numbers
LIMIT n
;


select *
from events_campaign
where tenant_id = 1
    and ;