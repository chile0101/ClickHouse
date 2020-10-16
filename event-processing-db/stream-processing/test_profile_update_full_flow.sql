faust -A prile.workflows.profile_update.app worker -l info -p 6067


faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":25, "anonymous_id": "a1", "identity_keys":["user_id"], "identity_vals" : ["1"], "str_properties_keys":["gender"],"str_properties_vals":["male"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":[],"arr_properties_vals":[],"at":1600928968}'



select * from user_profile where tenant_id = 1 and anonymous_id = 'a1';

select * from user_profile_final_v;



faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":25, "anonymous_id": "a1", "event_name": "View Product", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency","campaign_name"],"str_properties_vals":["VND","c1"],"num_properties_keys":["total_value"],"num_properties_vals":[2],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600928968}'







faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":25, "anonymous_id": "a1", "identity_keys":["user_id"], "identity_vals" : ["1"], "str_properties_keys":["gender"],"str_properties_vals":["male"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":[],"arr_properties_vals":[],"at":1600928968}'
faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":25, "anonymous_id": "a1", "identity_keys":["user_id", "email"], "identity_vals" : ["user_1","chilevan74@gmail.com"], "str_properties_keys":["gender","location_city"],"str_properties_vals":["male","Ho Chi Minh"],"num_properties_keys":["first_activity"],"num_properties_vals":[1600931072],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600931072}'

select * from user_profile_final_v where tenant_id = 25 and anonymous_id = 'a1';

truncate table user_profile_final;
truncate table user_profile;
select now();
select FROM_UNIXTIME(1601344766);
select FROM_UNIXTIME(1601345105);
select toUnixTimestamp(now()); 1601347985
select toUnixTimestamp(toStartOfDay(now())); 1601337600
select * from user_profile_final_v where tenant_id = 1 and anonymous_id = 'a1';

faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":1, "anonymous_id": "a1", "identity_keys":["user_id", "email"], "identity_vals" : ["user_1","chilevan74@gmail.com"], "str_properties_keys":["gender","city"],"str_properties_vals":["male","Ho Chi Minh"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":["channels"],"arr_properties_vals":[["facebook","google"]],"at":1601344766}'
a1,1,"['user_id','email']","['user_1','chilevan74@gmail.com']","['gender','city']","['male','Ho Chi Minh']",[],[],['channels'],"[['facebook','google']]",2020-09-29 01:59:26

faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":1, "anonymous_id": "a1", "identity_keys":["user_id", "email","facebook_id"], "identity_vals" : ["user_1","chile74@gmail.com","KHK23"], "str_properties_keys":["gender","city"],"str_properties_vals":["Male","Ho Chi Minh"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":["channels"],"arr_properties_vals":[["google","mailchim"]],"at":1601345105}'
a1,1,"['user_id','email','facebook_id']","['user_1','chile74@gmail.com','KHK23']","['gender','city']","['Male','Ho Chi Minh']",[],[],['channels'],"[['google','mailchim']]",2020-09-29 02:05:05


faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":1, "anonymous_id": "a2", "identity_keys":["user_id", "email","facebook_id"], "identity_vals" : ["user_2","user2@gmail.com","KHK23"], "str_properties_keys":["gender","city"],"str_properties_vals":["Female","Ha Noi"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":["channels"],"arr_properties_vals":[["facebook","google"]],"at":1601345306}'


faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":1, "anonymous_id": "a4", "identity_keys":["user_id", "email","facebook_id"], "identity_vals" : ["user_3","user3@gmail.com","KHK23"], "str_properties_keys":["gender","city"],"str_properties_vals":["Female","Ha Noi"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":["channels"],"arr_properties_vals":[["facebook","google"]],"at":1601337600}'

;
----------------TEST ON SERVER
truncate table user_profile_final;
truncate table user_profile;
select * from user_profile_final where tenant_id = 1 and anonymous_id = 'a1';
select * from user_profile_final_v where tenant_id = 1 and  anonymous_id = 'a1';

faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":1, "anonymous_id": "a1", "identity_keys":["user_id", "email","facebook_id"], "identity_vals" : ["user_1","user1@gmail.com","KHK23"], "str_properties_keys":["gender","city"],"str_properties_vals":["Female","Ha Noi"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":["channels"],"arr_properties_vals":[["facebook","google"]],"at":1601337600}'

faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":1, "anonymous_id": "a1", "identity_keys":["user_id", "email","facebook_id"], "identity_vals" : ["chile","chile@gmail.com","KHK23"], "str_properties_keys":["gender","city"],"str_properties_vals":["Male","Ho Chi Minh"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":["channels"],"arr_properties_vals":[["facebook","mailchim"]],"at":1601337605}'


faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":1, "anonymous_id": "a2", "identity_keys":["user_id", "email","facebook_id"], "identity_vals" : ["chile","chile@gmail.com","KHK23"], "str_properties_keys":["gender","city"],"str_properties_vals":["Male","Ho Chi Minh"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":["channels"],"arr_properties_vals":[["facebook","mailchim"]],"at":1601337705}'


-- WORK OK.
