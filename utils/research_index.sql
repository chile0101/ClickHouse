show tables;


SELECT
    `tenant_id`,
    anonymous_id,
    utm_campaign,
    `num_properties.vals`[indexOf(`num_properties.keys`, '{EventsCampaignKeys.TOTAL_VALUE}')] AS total_value
FROM events_campaign
WHERE tenant_id = :tenant_id
    AND utm_campaign = :utm_campaign
    AND toUnixTimestamp(at) >= :start_time
    AND toUnixTimestamp(at) <= :end_time;


drop table profile_final;
select * from profile_final;


select * from events_campaign where anonymous_id = 'a1';