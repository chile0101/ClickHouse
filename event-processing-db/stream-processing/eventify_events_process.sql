faust -A prile.workflows.eventify_events.app worker -l info -p 6066

faust -A prile.workflows.eventify_events.app send "eventify_event_topic"  '{"tenant_id":1,"event_id": "event1", "anonymous_id": "a1", "event_name": "Buy Xiaomi 12", "identity_keys":["user_id","email"], "identity_vals" : ["user-chi","user1@gmail.com"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[64000],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["v1","v2"]],"at":1601366065}'


select toUnixTimestamp(now());
select FROM_UNIXTIME(1601366065);
truncate table events;
select * from events;



    SELECT uniqMerge(value) AS total_user
    FROM event_total_user_by_source_ts
    WHERE tenant_id = 1 AND source_id = 'google'
    GROUP BY
        tenant_id,
        source_id
;

show create table event_total_user_by_source_ts;
show create table event_total_user_by_source_ts_mv;


select * from event_total_user_by_source_ts;

select now();
select toUnixTimestamp(now());

faust -A prile.workflows.eventify_events.app send "eventify_event_topic" '{"id": "id-x", "tenant_id":1, "anonymous_id": "a1", "event_name": "Buy Xiaomi 12", "identity_keys":["user_id","email"], "identity_vals" : ["user-chi","user1@gmail.com"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[64000],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["v1","v2"]],"at":1601540035}'

faust -A prile.workflows.eventify_events.app send "eventify_event_topic"  '{"id": "event-x","tenant_id":1, "anonymous_id": "a1", "event_name": "Buy Xiaomi 7", "identity_keys":[], "identity_vals" : [], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[64000],"arr_properties_keys":[],"arr_properties_vals":[],"at":1601540035}'


faust -A prile.workflows.eventify_events.app send "eventify_event_topic"  '{"id": "event-x","tenant_id":1, "anonymous_id": "a1", "event_name": "Buy Xiaomi 7", "identity_keys":[], "identity_vals" : [], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[64000],"arr_properties_keys":[],"arr_properties_vals":[],"at":1601540035}'



truncate table events;
select * from events where tenant_id = 1 and anonymous_id = 'a1';

---- TEST ON SERVER
truncate table events;
select * from events;
faust -A prile.workflows.eventify_events.app send "eventify_event_topic"  '{"id":"event-123","tenant_id":1, "anonymous_id": "a1", "event_name": "Buy Xiaomi 123", "identity_keys":[], "identity_vals" : [], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[64000],"arr_properties_keys":[],"arr_properties_vals":[],"at":1601543691}'



faust -A prile.workflows.eventify_events.app send "eventify_event_topic"  '{"id":"event_1234","tenant_id":1, "anonymous_id": "a1", "event_name": "App Uninstalled", "identity_keys":[], "identity_vals" : [], "str_properties_keys":["currency", "test_str"],"str_properties_vals":["VND","str_val"],"num_properties_keys":["total_value","float_key"],"num_properties_vals":[64000,123.12345678],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["v1","v2","v3","v4"]],"at":1601543691}'
