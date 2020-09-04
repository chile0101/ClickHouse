faust -A prile.workflows.profile_update.app worker -l info


faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":1, "anonymous_id": "a1", "identity_keys":["user_id"], "identity_vals" : ["1"], "str_properties_keys":["gender"],"str_properties_vals":["male"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":[""],"arr_properties_vals":[[]],"at":1598085203}'



select * from user_profile;

select * from user_profile_final;