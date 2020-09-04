faust -A prile.workflows.conversion_broadcast.app worker -l info -p 6068

faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "View Product", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency","campaign_name"],"str_properties_vals":["VND","c1"],"num_properties_keys":["total_value"],"num_properties_vals":[2],"arr_properties_keys":[""],"arr_properties_vals":[[]],"at":1598931880}'

