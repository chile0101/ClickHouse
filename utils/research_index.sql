show tables;


SELECT
    `tenant_id`,
    anonymous_id,
    utm_campaign
FROM events_campaign
WHERE  toUnixTimestamp(at) >=1606262400
    AND toUnixTimestamp(at) <= 1607212800;
--302 ms


SELECT
    `tenant_id`,
    anonymous_id,
    utm_campaign
FROM events_campaign
WHERE  at >= '2020-11-25 00:00:00'
    AND at <= '2020-12-06 00:00:00';

select * from events_campaign ;


select toUnixTimestamp( '2020-11-25 00:00:00'),
       toUnixTimestamp('2020-12-06 00:00:00')