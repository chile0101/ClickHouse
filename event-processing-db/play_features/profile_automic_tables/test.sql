insert into events_campaign
select
    id, tenant_id, event_name, event_name_type, anonymous_id, session_id, scope, '123Test123Test' as utm_campaign,
       utm_medium, utm_source, utm_content, utm_term,`str_properties.keys`, `str_properties.vals`,
        `num_properties.keys`, `num_properties.vals`,`arr_properties.keys`, `arr_properties.vals`,
        at + 60*60*24*10 as at, at_milli + 60*60*24*10 as at_milli
 from events_campaign;




select * from events_campaign;


select * from events_campaign;