select * from events_campaign;
utm_campaign = 'ARPU Increase Experiment'
select toUnixTimestamp('2020-11-06 00:00:00'); -- 1604707200
select toUnixTimestamp('2020-11-09 00:00:00'); -- 1604880000




SELECT tenant_id,
       anonymous_id,
       event_name,
       event_name_type,
       utm_campaign,
       utm_content,
       utm_source,
       total_value,
       time_stamp,
       gender,
       location_city,
       device
FROM
(
    SELECT
        tenant_id,
        anonymous_id,
        event_name,
        event_name_type,
        `campaign.vals`[indexOf(`campaign.keys`, 'utm_campaign')] AS utm_campaign,
        `campaign.vals`[indexOf(`campaign.keys`, 'utm_content')] AS utm_content,
        `campaign.vals`[indexOf(`campaign.keys`, 'utm_source')] AS utm_source,
        `num_properties.vals`[indexOf(`num_properties.keys`, 'properties.total_value')] AS total_value,
        toUnixTimestamp(toStartOfDay(created_at)) AS time_stamp
    FROM events_campaign
    WHERE tenant_id = 1
      AND utm_campaign = 'ARPU Increase Experiment'
      AND toUnixTimestamp(created_at) >= 1604620800
      AND toUnixTimestamp(created_at) <= 1604880000
) AS ec
INNER JOIN
(
    SELECT
        tenant_id,
        anonymous_id,
        `str_pros_vals`[indexOf(`str_pros_keys`, 'gender')] AS gender,
        `str_pros_vals`[indexOf(`str_pros_keys`, 'city')] AS location_city,
        `str_pros_vals`[indexOf(`str_pros_keys`, 'device_platform')] AS device
    FROM user_profile_final_v
    WHERE
          tenant_id = 1
          AND anonymous_id IN
            (
                SELECT anonymous_id
                FROM events_campaign
                WHERE `campaign.vals`[indexOf(`campaign.keys`, 'utm_campaign')] = 'ARPU Increase Experiment'
            )
) AS pf
ON ec.tenant_id = pf.tenant_id and ec.anonymous_id = pf.anonymous_id
;


----
select toDateTime(toDate('2020-10-09'));


select distinct tenant_id from user_profile;
select distinct tenant_id from events;


['ARPU Increase Experiment','facebook','cpc','Content Tactics B 30% discount']

;





select * from events_campaign order by created_at desc ;