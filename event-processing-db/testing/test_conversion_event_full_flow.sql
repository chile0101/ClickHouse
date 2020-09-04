faust -A prile.workflows.conversion_broadcast.app worker -l info -p 6067


faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":2, "anonymous_id": "a1", "event_name": "View", "identity_keys":["user_id"], "identity_vals" : ["1"], "str_properties_keys":["gender"],"str_properties_vals":["male"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":[""],"arr_properties_vals":[[]],"at":1598085203}'




faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "View Product", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[12],"arr_properties_keys":[""],"arr_properties_vals":[[]],"at":1598085203}'



faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "View Product", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[1],"arr_properties_keys":[""],"arr_properties_vals":[[]],"at":1598931880}'

faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "View Product", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[1],"arr_properties_keys":[""],"arr_properties_vals":[[]],"at":1598931880}'




insert into user_profile values
('a1' , 1, ['email'],['chi@'], ['gender'],['male'],[],[],[],[], now())
;




select * from user_profile;

SELECT
     anonymous_id,
     tenant_id ,
    argMaxMerge(identity.keys) AS identity_keys,
    argMaxMerge(identity.vals) AS identity_vals,
    argMaxMerge(str_properties.keys) AS str_pros_keys,
    argMaxMerge(str_properties.vals) AS str_pros_vals,
    argMaxMerge(num_properties.keys) AS num_pros_keys,
    argMaxMerge(num_properties.vals) AS num_pros_vals,
    argMaxMerge(arr_properties.keys) AS arr_pros_keys,
    argMaxMerge(arr_properties.vals) AS arr_pros_vals,
    max(at_final) AS at
FROM user_profile_final
GROUP BY tenant_id, anonymous_id;


truncate table user_profile;

truncate table user_profile_final;

select toUnixTimestamp(now())

