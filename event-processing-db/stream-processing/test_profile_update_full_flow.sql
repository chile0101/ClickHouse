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


-- RETEST - DATA SAMPLE

select * from user_profile;



faust -A prile.workflows.profile_update.app send "profile-updated" '{"anonymous_i": "DBv6DnUox6tUU8wpetWC9Bw0Oo6", "tenant_id": 0, "identity_keys": [], "identity_vals": [], "str_properties_keys": ["lastVisit"], "str_properties_vals": ["2020-10-20T02:14:38Z"], "num_properties_keys": ["nbOfVisits", "firstVisit"], "num_properties_vals": [1, 1603160100000], "arr_properties_keys": [], "arr_properties_vals": [], "at": 1603163207}'




select *
from user_profile_final_v
 where tenant_id = 0 and anonymous_id = 'a1'
order by at desc;



--------------------------------Convert to datetime 64

faust -A prile.workflows.profile_update.app send "profile-updated" '{"anonymous_id":"DC2jcXQnSJhAr2rOA7c6v4ykT8E","tenant_id":1,"identity_keys":[],"identity_vals":[],"str_properties_keys":["firstVisit","lastVisit","systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_0_0StartReached","systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_0_0TargetReached","systems.goals.1jjOK0l94swc1SXV51qktZlsmfu_0_0StartReached","systems.goals.1jjOK0l94swc1SXV51qktZlsmfu_0_0TargetReached","systems.goals.1jjOK0l94swc1SXV51qktZlsmfu_0_1StartReached","systems.goals.1jjVFF6tiy7bhhJzw84yac8QvDm_0_0StartReached","systems.goals.1jjVFF6tiy7bhhJzw84yac8QvDm_0_0TargetReached","systems.goals.1jjVFF6tiy7bhhJzw84yac8QvDm_0_1StartReached","systems.lastUpdated"],"str_properties_vals":["2020-11-02T11:45:53Z","2020-11-02T11:45:53Z","2020-11-02T11:45:57Z","2020-11-02T11:45:58Z","2020-11-02T11:45:57Z","2020-11-02T11:45:58Z","2020-11-02T11:45:58Z","2020-11-02T11:45:57Z","2020-11-02T11:45:58Z","2020-11-02T11:45:58Z","2020-11-02T11:45:56Z"],"num_properties_keys":["nbOfVisits"],"num_properties_vals":[1],"arr_properties_keys":[],"arr_properties_vals":[],"at":1604317558191}'

select now64();
select toUnixTimestamp64Milli(now64());-- 1604331595254
select FROM_UNIXTIME(1604331595); -- 2043-03-01 03:37:26



faust -A prile.workflows.profile_update.app send "profile-updated" '{"tenant_id": 1, "anonymous_id": "DC539BRX9CYcvtkgMnGINv6LcaB", "identity_keys": ["email"], "identity_vals": ["ehanhartf8@businesswire.com"], "str_properties_keys": ["country", "id", "last_name", "profileimage", "device_make", "city", "firstVisit", "dob", "device_os_version", "first_name", "lastVisit", "registrationstartdate", "appversion", "loyalty_status", "house_number", "device_platform", "preferred_language", "street", "timezone", "gender", "device_model", "systems.goals.1jiMLWT1ZDMCrZa97aLDGobTaxH_0_0StartReached", "systems.goals.1jio9MsQBBLutN0n1LA4HfrU9pr_0_1TargetReached", "systems.goals.1jgrwemEq2hFvxTx0Xkjx9ELgkD_0_0StartReached", "systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached", "systems.goals.1jjVFF6tiy7bhhJzw84yac8QvDm_0_1StartReached", "systems.goals.1jjOK0l94swc1SXV51qktZlsmfu_0_1TargetReached", "systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0StartReached", "systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0TargetReached", "systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0StartReached", "systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_1TargetReached", "systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0TargetReached", "systems.goals.1jS5JpDOl65xE5BlroVkJZot76h_0_0TargetReached", "systems.goals.1jP1RhZyXPnkRUsRGlfkUVAlLpD_0StartReached", "systems.goals.1jS4Rbl6dkv36aXQfWQaVHLxd5H_0_0TargetReached", "systems.goals.1jivOJ0LI3zlukOUwt7DvTr61zz_0_0TargetReached", "systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0TargetReached", "systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0TargetReached", "systems.goals.1jXchvHca1c6uiSpuACrUiRzrUJ_0_0TargetReached", "systems.goals.1jS4Rbl6dkv36aXQfWQaVHLxd5H_0_1StartReached", "systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0TargetReached", "systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_0_0StartReached", "systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached", "systems.goals.1jjeyqNkAnrfVyXYdv5bg2zPtgK_0_0StartReached", "systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0TargetReached", "systems.goals.1jS5JpDOl65xE5BlroVkJZot76h_1_0StartReached", "systems.goals.1jXedSf8jHXH5tNapptC7arlz8s_0_0StartReached", "systems.goals.1jRzsvB5oj86EWtgMe0OSzgLboZ_0_1TargetReached", "systems.goals.1jS5JpDOl65xE5BlroVkJZot76h_1_0TargetReached", "systems.goals.1jjeyqNkAnrfVyXYdv5bg2zPtgK_0_0TargetReached", "systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0TargetReached", "systems.goals.1jS0xAwNptdshnHygZbv5jTesPu_0_1StartReached", "systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0StartReached", "systems.goals.1jjAr7I8uuCL3i4JBPMPe6mg91X_0_1TargetReached", "systems.goals.1jjVFF6tiy7bhhJzw84yac8QvDm_0_0StartReached", "systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0StartReached", "systems.goals.1jio9MsQBBLutN0n1LA4HfrU9pr_0_0TargetReached", "systems.goals.1jjOK0l94swc1SXV51qktZlsmfu_0_1StartReached", "systems.goals.1jXsqzMx9owAGqxqZI5SUoGsI7r_0_0TargetReached", "systems.goals.1jio9MsQBBLutN0n1LA4HfrU9pr_0_0StartReached", "systems.goals.1jiaG4UMPFQIl7ZS4m372L1FhJ6_0_0StartReached", "systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0StartReached", "systems.goals.1jXsqzMx9owAGqxqZI5SUoGsI7r_0_0StartReached", "systems.goals.1jio9MsQBBLutN0n1LA4HfrU9pr_0_1StartReached", "systems.goals.1jRc6h5OaaZPRtYp1dZIuayPkGF_0_0StartReached", "systems.goals.1jjVFF6tiy7bhhJzw84yac8QvDm_0_0TargetReached", "systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_0_0StartReached", "systems.goals.1jRzsvB5oj86EWtgMe0OSzgLboZ_0_1StartReached", "systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0TargetReached", "systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0StartReached", "systems.goals.1jS0xAwNptdshnHygZbv5jTesPu_0_1TargetReached", "systems.goals.1jS0xAwNptdshnHygZbv5jTesPu_0_0StartReached", "systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached", "systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_0_0TargetReached", "systems.goals.1jjAr7I8uuCL3i4JBPMPe6mg91X_0_0TargetReached", "systems.goals.1jS5JpDOl65xE5BlroVkJZot76h_0_0StartReached", "systems.goals.1jlJM7WNeosQuXD7dGSEyC9ZOBy_0_0TargetReached", "systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0TargetReached", "systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0StartReached", "systems.goals.1jlJM7WNeosQuXD7dGSEyC9ZOBy_0_0StartReached", "systems.goals.1jjAr7I8uuCL3i4JBPMPe6mg91X_0_1StartReached", "systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0StartReached", "systems.goals.1jiMLWT1ZDMCrZa97aLDGobTaxH_0_0TargetReached", "systems.goals.1jimK7w6tAuunXM2vsqlLbj4U57_0_0StartReached", "systems.goals.1jXedSf8jHXH5tNapptC7arlz8s_0_0TargetReached", "systems.goals.1jS4Rbl6dkv36aXQfWQaVHLxd5H_0_0StartReached", "systems.goals.1jS4Rbl6dkv36aXQfWQaVHLxd5H_0_1TargetReached", "systems.goals.1jRzsvB5oj86EWtgMe0OSzgLboZ_0_0TargetReached", "systems.goals.1jimK7w6tAuunXM2vsqlLbj4U57_0_0TargetReached", "systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0StartReached", "systems.goals.1jiaG4UMPFQIl7ZS4m372L1FhJ6_0_0TargetReached", "systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_1StartReached", "systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_0_0TargetReached", "systems.goals.1jjOK0l94swc1SXV51qktZlsmfu_0_0TargetReached", "systems.goals.1jRzsvB5oj86EWtgMe0OSzgLboZ_0_0StartReached", "systems.goals.1jXchvHca1c6uiSpuACrUiRzrUJ_0_0StartReached", "systems.goals.1jjAr7I8uuCL3i4JBPMPe6mg91X_0_0StartReached", "systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0TargetReached", "systems.goals.1jS0xAwNptdshnHygZbv5jTesPu_0_0TargetReached", "systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0TargetReached", "systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0StartReached", "systems.goals.1jivOJ0LI3zlukOUwt7DvTr61zz_0_0StartReached", "systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached", "systems.goals.1jjVFF6tiy7bhhJzw84yac8QvDm_0_1TargetReached", "systems.goals.1jjOK0l94swc1SXV51qktZlsmfu_0_0StartReached", "systems.lastUpdated", "properties.currency"], "str_properties_vals": ["Portugal", "4pZaQGW", "Hanhart", "https://robohash.org/situndelaudantium.jpg?size=100x100&set=set1", "Mercedes-Benz", "Madalena do Mar", "2020-11-03T02:52:40Z", "04/06/1992", "0.51", "Ethelyn", "2020-11-03T02:52:41Z", "04/10/2015", "0.5.4", "Diamond", "0", "Android", "Hebrew", "Cody", "Africa/Accra", "Female", "SL-Class", "2020-11-03T02:52:43Z", "2020-11-03T02:52:51Z", "2020-11-03T02:52:44Z", "2020-11-03T02:52:45Z", "2020-11-03T02:52:51Z", "2020-11-03T02:53:57Z", "2020-11-03T02:52:45Z", "2020-11-03T02:52:48Z", "2020-11-03T02:54:01Z", "2020-11-03T02:55:44Z", "2020-11-03T02:54:03Z", "2020-11-03T02:52:45Z", "2020-11-03T02:54:01Z", "2020-11-03T02:55:40Z", "2020-11-03T02:54:01Z", "2020-11-03T02:54:03Z", "2020-11-03T02:52:51Z", "2020-11-03T03:33:16Z", "2020-11-03T02:55:40Z", "2020-11-03T02:54:01Z", "2020-11-03T02:52:48Z", "2020-11-03T02:52:45Z", "2020-11-03T02:52:50Z", "2020-11-03T02:54:03Z", "2020-11-03T02:52:43Z", "2020-11-03T02:54:01Z", "2020-11-03T03:33:18Z", "2020-11-03T02:52:48Z", "2020-11-03T02:52:51Z", "2020-11-03T02:52:50Z", "2020-11-03T02:52:45Z", "2020-11-03T02:54:01Z", "2020-11-03T02:52:51Z", "2020-11-03T02:52:50Z", "2020-11-03T02:52:44Z", "2020-11-03T02:52:50Z", "2020-11-03T02:52:51Z", "2020-11-03T02:52:51Z", "2020-11-03T02:52:48Z", "2020-11-03T02:52:43Z", "2020-11-03T02:52:44Z", "2020-11-03T02:52:45Z", "2020-11-03T02:52:50Z", "2020-11-03T03:39:06Z", "2020-11-03T02:52:51Z", "2020-11-03T02:52:50Z", "2020-11-03T03:33:16Z", "2020-11-03T02:54:01Z", "2020-11-03T02:54:01Z", "2020-11-03T02:54:01Z", "2020-11-03T02:52:43Z", "2020-11-03T02:52:44Z", "2020-11-03T02:52:51Z", "2020-11-03T02:52:45Z", "2020-11-03T02:52:43Z", "2020-11-03T03:27:39Z", "2020-11-03T02:52:47Z", "2020-11-03T02:52:45Z", "2020-11-03T03:27:38Z", "2020-11-03T02:52:45Z", "2020-11-03T02:52:45Z", "2020-11-03T02:52:44Z", "2020-11-03T02:52:44Z", "2020-11-03T03:33:16Z", "2020-11-03T02:54:01Z", "2020-11-03T02:55:47Z", "2020-11-03T03:33:16Z", "2020-11-03T02:52:45Z", "2020-11-03T02:52:45Z", "2020-11-03T02:53:57Z", "2020-11-03T02:55:41Z", "2020-11-03T02:52:51Z", "2020-11-03T02:52:51Z", "2020-11-03T02:54:01Z", "2020-11-03T02:54:01Z", "2020-11-03T02:52:44Z", "2020-11-03T02:55:41Z", "2020-11-03T02:52:45Z", "2020-11-03T02:54:01Z", "2020-11-03T02:54:01Z", "2020-11-03T02:52:43Z", "2020-11-03T02:52:45Z", "2020-11-03T02:53:57Z", "2020-11-03T02:52:50Z", "2020-11-03T03:40:25Z", "VND"], "num_properties_keys": ["age", "lat", "nbOfVisits", "long", "total_order", "last_order_at", "avg_order_value", "revenue"], "num_properties_vals": [40.0, 32.701088, 1.0, -17.134052, 1, 1604375201910, 148845, 148845], "arr_properties_keys": ["segments"], "arr_properties_vals": [["1jPKiEG8X2GZ3tIUH8WGEia38l0", "1jPN77SZbyFj2aYoeirVxGGcKJD"]], "at": 1604375201910, "__faust": {"ns": "prile.workflows.profile_update.events.ProfileUpdate"}}'
