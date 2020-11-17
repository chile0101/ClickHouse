with
    `str_properties.vals`[indexOf(`str_properties.keys`, 'utm_campaign')] as campaign_id
select
*
from events_campaign
where tenant_id = 1
  and campaign_id = 'campaign2'