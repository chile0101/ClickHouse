segment_id = '1jP08tvZXG9vJj7uJ4okEwCCLlE'
DCVQnEBT4bVEGjU9BJa4ATYgqZu
DB06SZESUs5rEQ108zeg6Au1Voo
DCVQpSO6b6o3b7zs9Alax9BSgYp
DB80ga9Cn18JZM7t9A81w6g6SRo
DCQQ9AmHfoGKLjKrlKljtb02zen
DCbYfmAi29A3Skobl46tuLfmQil
select toUnixTimestamp(now()); -- 1603783531
select now(); -- 2020-10-27 07:26:16
select distinct anonymous_id from user_profile_final_v limit 30;


faust -A prile.workflows.segments_users_update.app send "segment-user-update"  '{"segment":"1jVMk40iodyvOukinjcN9BeSSSk","state":"segmentOptIn","user":"DCQP9Ckh0hBxL1W6IPJFjg21kgQ","at":1603894425,"tenant_id":1}

--- segmnent user update

faust -A prile.workflows.segments_users_update.app send "segment-user-update"  '{"tenant_id":1, "segment":"1jP08tvZXG9vJj7uJ4okEwCCLlE", "state": "segmentOptIn", "user":"DCVQnEBT4bVEGjU9BJa4ATYgqZu", "at":"1603772551"}'
faust -A prile.workflows.segments_users_update.app send "segment-user-update"  '{"tenant_id":1, "segment":"1jP08tvZXG9vJj7uJ4okEwCCLlE", "state": "segmentOptIn", "user":"DB06SZESUs5rEQ108zeg6Au1Voo", "at":"1603772552"}'
faust -A prile.workflows.segments_users_update.app send "segment-user-update"  '{"tenant_id":1, "segment":"1jP08tvZXG9vJj7uJ4okEwCCLlE", "state": "segmentOptIn", "user":"DCVQpSO6b6o3b7zs9Alax9BSgYp", "at":"1603772553"}'

faust -A prile.workflows.segments_users_update.app send "segment-user-update"  '{"tenant_id":1, "segment":"1jP08tvZXG9vJj7uJ4okEwCCLlE", "state": "segmentOptIn", "user":"DB80ga9Cn18JZM7t9A81w6g6SRo", "at":"1603772560"}'

faust -A prile.workflows.segments_users_update.app send "segment-user-update"  '{"tenant_id":1, "segment":"1jP08tvZXG9vJj7uJ4okEwCCLlE", "state": "segmentOptIn", "user":"DCQQ9AmHfoGKLjKrlKljtb02zen", "at":"1603772600"}'' ||
faust -A prile.workflows.segments_users_update.app send "segment-user-update"  '{"tenant_id":1, "segment":"1jP08tvZXG9vJj7uJ4okEwCCLlE", "state": "segmentOptIn", "user":"DCbYfmAi29A3Skobl46tuLfmQil", "at":"1603900347"}'

1603772600
1603900347
select FROM_UNIXTIME(1603772600);
select toUnixTimestamp(now());
select now();

select * from segment_users_final_v where segment_id = '1jP08tvZXG9vJj7uJ4okEwCCLlE';

--- Check segment user update table
select * from segment_users where segment_id = '1jP08tvZXG9vJj7uJ4okEwCCLlE';
select * from segment_users_final_v where segment_id = '1jP08tvZXG9vJj7uJ4okEwCCLlE';

-- push conversion event.
faust -A prile.workflows.conversion_broadcast.app send "conversion-events" '{"eventType":"checkout_completed","itemId":"DB06SZESUs5rEQ108zeg6Au1Voo","itemType":"event","persistent":true,"profileId":"DB06SZESUs5rEQ108zeg6Au1Voo","properties":{"cart_id":"1","cart_subtotal_value":0,"country":"VI","coupon_added":false,"currency":"vnd","discount_value":0,"gender":"male","order_id":"1","payment_method":"COD","shipping_address":"vi","shipping_cost":0,"shipping_country":"VI","shipping_state":"vi","shipping_zipcode":"70000","total_value":20},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","sendAt":1603777191883,"sessionId":"fe5fd438-fdab-bf59-e1f0-8e773c0f1323","source":{"itemId":"https://weburnit.github.io/","itemType":"page","properties":{"attributes":[],"consentTypes":[],"interests":{},"pageInfo":{"destinationURL":"https://weburnit.github.io/","pageName":"Pullman Hotel Resorts - Saloniz Solution","pagePath":"/","referringURL":""}},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","version":null},"target":{"itemId":"/","itemType":"page","properties":{},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","version":null},"tenant_id":1,"timeStamp":1603783531,"version":null}'
faust -A prile.workflows.conversion_broadcast.app send "conversion-events" '{"eventType":"checkout_completed","itemId":"DB06SZESUs5rEQ108zeg6Au1Voo","itemType":"event","persistent":true,"profileId":"DCVQnEBT4bVEGjU9BJa4ATYgqZu","properties":{"cart_id":"1","cart_subtotal_value":0,"country":"VI","coupon_added":false,"currency":"vnd","discount_value":0,"gender":"male","order_id":"1","payment_method":"COD","shipping_address":"vi","shipping_cost":0,"shipping_country":"VI","shipping_state":"vi","shipping_zipcode":"70000","total_value":10},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","sendAt":1603777191883,"sessionId":"fe5fd438-fdab-bf59-e1f0-8e773c0f1323","source":{"itemId":"https://weburnit.github.io/","itemType":"page","properties":{"attributes":[],"consentTypes":[],"interests":{},"pageInfo":{"destinationURL":"https://weburnit.github.io/","pageName":"Pullman Hotel Resorts - Saloniz Solution","pagePath":"/","referringURL":""}},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","version":null},"target":{"itemId":"/","itemType":"page","properties":{},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","version":null},"tenant_id":1,"timeStamp":1603783535,"version":null}'
faust -A prile.workflows.conversion_broadcast.app send "conversion-events" '{"eventType":"checkout_completed","itemId":"DB06SZESUs5rEQ108zeg6Au1Voo","itemType":"event","persistent":true,"profileId":"DCVQpSO6b6o3b7zs9Alax9BSgYp","properties":{"cart_id":"1","cart_subtotal_value":0,"country":"VI","coupon_added":false,"currency":"vnd","discount_value":0,"gender":"male","order_id":"1","payment_method":"COD","shipping_address":"vi","shipping_cost":0,"shipping_country":"VI","shipping_state":"vi","shipping_zipcode":"70000","total_value":30},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","sendAt":1603777191883,"sessionId":"fe5fd438-fdab-bf59-e1f0-8e773c0f1323","source":{"itemId":"https://weburnit.github.io/","itemType":"page","properties":{"attributes":[],"consentTypes":[],"interests":{},"pageInfo":{"destinationURL":"https://weburnit.github.io/","pageName":"Pullman Hotel Resorts - Saloniz Solution","pagePath":"/","referringURL":""}},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","version":null},"target":{"itemId":"/","itemType":"page","properties":{},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","version":null},"tenant_id":1,"timeStamp":1603783532,"version":null}'

faust -A prile.workflows.conversion_broadcast.app send "conversion-events" '{"eventType":"checkout_completed","itemId":"DB06SZESUs5rEQ108zeg6Au1Voo","itemType":"event","persistent":true,"profileId":"DCVQpSO6b6o3b7zs9Alax9BSgYp","properties":{"cart_id":"1","cart_subtotal_value":0,"country":"VI","coupon_added":false,"currency":"vnd","discount_value":0,"gender":"male","order_id":"1","payment_method":"COD","shipping_address":"vi","shipping_cost":0,"shipping_country":"VI","shipping_state":"vi","shipping_zipcode":"70000","total_value":30},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","sendAt":1603777191883,"sessionId":"fe5fd438-fdab-bf59-e1f0-8e773c0f1323","source":{"itemId":"https://weburnit.github.io/","itemType":"page","properties":{"attributes":[],"consentTypes":[],"interests":{},"pageInfo":{"destinationURL":"https://weburnit.github.io/","pageName":"Pullman Hotel Resorts - Saloniz Solution","pagePath":"/","referringURL":""}},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","version":null},"target":{"itemId":"/","itemType":"page","properties":{},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","version":null},"tenant_id":1,"timeStamp":1603783532,"version":null}'


faust -A prile.workflows.conversion_broadcast.app send "conversion-events" '{"eventType":"checkout_completed","itemId":"DB06SZESUs5rEQ108zeg6Au1Voo","itemType":"event","persistent":true,"profileId":"DB80ga9Cn18JZM7t9A81w6g6SRo","properties":{"cart_id":"1","cart_subtotal_value":0,"country":"VI","coupon_added":false,"currency":"vnd","discount_value":0,"gender":"male","order_id":"1","payment_method":"COD","shipping_address":"vi","shipping_cost":0,"shipping_country":"VI","shipping_state":"vi","shipping_zipcode":"70000","total_value":20},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","sendAt":1603777191883,"sessionId":"fe5fd438-fdab-bf59-e1f0-8e773c0f1323","source":{"itemId":"https://weburnit.github.io/","itemType":"page","properties":{"attributes":[],"consentTypes":[],"interests":{},"pageInfo":{"destinationURL":"https://weburnit.github.io/","pageName":"Pullman Hotel Resorts - Saloniz Solution","pagePath":"/","referringURL":""}},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","version":null},"target":{"itemId":"/","itemType":"page","properties":{},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","version":null},"tenant_id":1,"timeStamp":1603584260,"version":null}'


select FROM_UNIXTIME(1603784);

-- Check update conversion in user profile table
select *
from user_profile_final_v
where tenant_id = 1 and anonymous_id = 'DCQQ9AmHfoGKLjKrlKljtb02zen' ;



-- Saw revenue on UI


-- Test profile update gender location source
faust -A prile.workflows.profile_update.app send "profile-updated" '{"anonymous_id": "DCQQ9AmHfoGKLjKrlKljtb02zen", "tenant_id": 1, "identity_keys": [], "identity_vals": [], "str_properties_keys": ["lastVisit","gender","location_city"], "str_properties_vals": ["2020-10-20T02:14:38Z","Female", "Ha Noi"], "num_properties_keys": ["nbOfVisits", "firstVisit"], "num_properties_vals": [1, 1603160100000], "arr_properties_keys": ["channels"], "arr_properties_vals": [["google","facebook"]], "at": 1603896868}'



['firstVisit','lastVisit','systems.lastUpdated','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached']

['2020-10-27T04:14:20Z','2020-10-27T04:14:20Z','2020-10-27T04:14:23Z','2020-10-27T04:14:23Z','2020-10-27T04:14:24Z','2020-10-27T04:14:24Z','2020-10-27T04:14:24Z']



----------TEST PROFILE UPDATE
faust -A prile.workflows.profile_update.app send "profile-updated" '{"anonymous_id":"DBXMHxUZxmV6mqVtwbZAaiC7wdc","tenant_id":1,"identity_keys":[],"identity_vals":[],"str_properties_keys":["lastVisit"],"str_properties_vals":["2020-10-15T09:45:03Z"],"num_properties_keys":["nbOfVisits","firstVisit"],"num_properties_vals":[1,1602755100000],"arr_properties_keys":[],"arr_properties_vals":[],"at":1602755103}'





faust -A prile.workflows.profile_update.app send "profile-updated" '{"anonymous_id": "DCbYfmAi29A3Skobl46tuLfmQil", "tenant_id": 0, "identity_keys": [], "identity_vals": [], "str_properties_keys": ["lastVisit"], "str_properties_vals": ["2020-10-20T02:14:38Z"], "num_properties_keys": ["nbOfVisits", "firstVisit"], "num_properties_vals": [1, 1603160100000], "arr_properties_keys": [], "arr_properties_vals": [], "at": 1603900347}'


select count(*) from user_profile_final_v;
select distinct anonymous_id from user_profile limit 30;


 Profiles [{'anonymous_id': 'DCVQnEBT4bVEGjU9BJa4ATYgqZu', 'tenant_id': 1, 'at': datetime.datetime(2020, 10, 27, 4, 23, 22), 'identity.keys': [], 'identity.vals': [], 'str_properties.keys': ['firstVisit', 'lastVisit', 'systems.lastUpdated', 'systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached', 'systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached', 'systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached', 'systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached', 'properties.currency', 'gender', 'location_city'], 'str_properties.vals': ['2020-10-27T04:14:20Z', '2020-10-20T02:14:38Z', '2020-10-27T04:14:23Z', '2020-10-27T04:14:23Z', '2020-10-27T04:14:24Z', '2020-10-27T04:14:24Z', '2020-10-27T04:14:24Z', 'VND', 'Male', 'Ho Chi Minh'], 'num_properties.keys': ['nbOfVisits', 'total_order', 'last_order_at', 'avg_order_value', 'revenue', 'firstVisit'], 'num_properties.vals': [1, 1.0, 1603777193.0, 20.0, 20.0, 1603160100000], 'arr_properties.keys': ['channels'], 'arr_properties.vals': [['google', 'facebook']]}] inserted to db successfully

select toUnixTimestamp(toDateTime('2020-10-27 06:00:00'));


select FROM_UNIXTIME(1603785990);


select * from segment_users_final_v where segment_id = '1jS6vp4IciwMvx2CucEwEAV07ZN';

select * from user_profile_final_v where tenant_id =1 and anonymous_id = 'DCWE9Cgx5yDPVvQy9ATFtKZ3A17';

select toUnixTimestamp(toDateTime('2020-10-20 00:00:00'));

select toUnixTimestamp(now());



select arrayZip(str_pros_keys, str_pros_vals) from user_profile_final_v order by at desc ;
['lastVisit','systems.mergeIdentifier','house_number','country','device_model','profileimage','device_os_version','gender','firstVisit','device_platform','id','device_make','preferred_language','last_name','registrationstartdate','appversion','timezone','city','dob','loyalty_status','street','first_name','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached','systems.lastUpdated','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached','systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0StartReached','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached']
['2020-10-28T03:32:34Z','145','145','Portugal','C-Class','https://robohash.org/molestiaeautnulla.jpg?size=100x100&set=set1','1.88','Female','2020-10-28T03:32:32Z','iOS','G0UZ2MG','Mercedes-Benz','Zulu','Klehn','10/10/2012','7.20','Europe/Lisbon','Alpiarça','06/03/1991','Gold','Fieldstone','Carmina','2020-10-28T03:32:37Z','2020-10-28T03:32:37Z','2020-10-28T03:32:36Z','2020-10-28T03:32:36Z','2020-10-28T03:32:38Z','2020-10-28T03:32:38Z']




select * from user_profile_final_v where anonymous_id = 'DCQQ9AmHfoGKLjKrlKljtb02zen';

sers_update.app send "segment-user-update"  '{"tenant_id":1, "segment":"1jP08tvZXG9vJj7uJ4okEwCCLlE", "state": "segmentOptIn", "user":"DCQREGlJEhCcEMtWb5LPzipjTcs" ,"at":"1603772600"}'




;


select * from user_profile_final_v order by at desc limit 50;
['lastVisit','systems.mergeIdentifier','loyalty_status','street','device_make','last_name','profileimage','preferred_language','id','gender','device_model','device_platform','house_number','city','device_os_version','first_name','timezone','dob','appversion','country','registrationstartdate','firstVisit','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached','systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0StartReached','systems.lastUpdated','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached','systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0StartReached','previousVisit','systems.goals.1jS5JpDOl65xE5BlroVkJZot76h_0_0StartReached','systems.goals.1jS5JpDOl65xE5BlroVkJZot76h_1_0StartReached','systems.goals.1jS0xAwNptdshnHygZbv5jTesPu_0_0StartReached','systems.goals.1jS0xAwNptdshnHygZbv5jTesPu_0_1StartReached','systems.goals.1jS0xAwNptdshnHygZbv5jTesPu_0_0TargetReached','systems.goals.1jS5JpDOl65xE5BlroVkJZot76h_0_0TargetReached','systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0StartReached']



select arrayDistinct(str_pros_keys)
from user_profile_final_v where anonymous_id = 'DCavCaog5eLSGFwi8f8mBes25iU';



['DCavCaog5eLSGFwi8f8mBes25iU']
select * from segment_users_final_v where segment_id = '1jXaAW4eEtNMd4Cs5YPgr6dqJkq';
select * from user_profile_final_v where anonymous_id = 'DCavCaog5eLSGFwi8f8mBes25iU';







['DCavCaog5eLSGFwi8f8mBes25iU']


select * from segment_users_final_v where segment_id = '1jXaAW4eEtNMd4Cs5YPgr6dqJkq';

select tenant_id, anonymous_id, num_pros_vals[indexOf(num_pros_keys, 'revenue')]
from user_profile_final_v
where tenant_id = 1 and  anonymous_id = 'DCavCaog5eLSGFwi8f8mBes25iU';


['' ||
'' ||
'' ||
'','DCavCaog5eLSGFwi8f8mBes25iU']

select * from segment_users_final_v where segment_id = '1jXaAW4eEtNMd4Cs5YPgr6dqJkq';

select * from user_profile_final_v where tenant_id = 1 and  anonymous_id = 'DCgcAhsLagJjAjenDHPOvW1tWlY';

select * from user_profile where tenant_id = 1 and  anonymous_id = 'DCgcAhsLagJjAjenDHPOvW1tWlY';








select * from user_profile where tenant_id = 1 and  anonymous_id = 'DCg1IXn9C72izQoxgRhIrPGokH2';






select arrayExists(x -> x )
from user_profile
where tenant_id and anonymous_id = 'DCgbGKCFeestpSrZLIx2pXsrZ1g';

['lastVisit','systems.mergeIdentifier','last_name','device_model','gender','country','city','device_make','post_code','timezone'
,'preferred_language','street','profile_image','device_os_version','acquisition_type','id','app_version',
'phone_number','device_platform','loyalty_status','house_number','registration_start_date','first_name',
'firstVisit','dob','systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0StartReached','systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0StartReached','systems.goals.1jXchvHca1c6uiSpuACrUiRzrUJ_0_0StartReached','systems.goals.1jXedSf8jHXH5tNapptC7arlz8s_0_0StartReached','systems.goals.1jP1RhZyXPnkRUsRGlfkUVAlLpD_0StartReached','systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0StartReached','systems.lastUpdated','systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0TargetReached','systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0TargetReached','systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0TargetReached']



----
['DCg5lymvV1UVegrVGyN1GQ5L9AK','DCg1IXn9C72izQoxgRhIrPGokH2']
select * from segment_users_final_v where segment_id = '1jXsqzMx9owAGqxqZI5SUoGsI7r';
select * from user_profile_final_v where anonymous_id = 'DCg5lymvV1UVegrVGyN1GQ5L9AK';
select * from user_profile where anonymous_id = 'DCg5lymvV1UVegrVGyN1GQ5L9AK' order by at desc ;

select FROM_UNIXTIME(1603962775);
select FROM_UNIXTIME(1603962831);
select FROM_UNIXTIME(1603962831);



show create table user_profile;

CREATE TABLE eventify.user_profile
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `identity.keys` Array(String),
    `identity.vals` Array(String),
    `str_properties.keys` Array(String),
    `str_properties.vals` Array(String),
    `num_properties.keys` Array(String),
    `num_properties.vals` Array(Float64),
    `arr_properties.keys` Array(String),
    `arr_properties.vals` Array(Array(String)),
    `at` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, anonymous_id, at)
;

alter table user_profile add column at_unix DateTime64;

select * from user_profile limit 10;

show create user_profile_final;

CREATE TABLE eventify.user_profile_final
(
    `anonymous_id` String,
    `tenant_id` UInt16,
    `identity.keys` AggregateFunction(argMax, Array(String), DateTime),
    `identity.vals` AggregateFunction(argMax, Array(String), DateTime),
    `str_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
    `str_properties.vals` AggregateFunction(argMax, Array(String), DateTime),
    `num_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
    `num_properties.vals` AggregateFunction(argMax, Array(Float64), DateTime),
    `arr_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
    `arr_properties.vals` AggregateFunction(argMax, Array(Array(String)), DateTime),
    `at_final` SimpleAggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree()
ORDER BY (tenant_id, anonymous_id)
SETTINGS index_granularity = 8192
;


[1603962800000,1,1,1603962831,100,100]



select * from user_profile_final_v where anonymous_id = 'DCrvz9BLRkUA8ovG9BmwxpeGH5e';
select * from user_profile where anonymous_id = 'DCrvz9BLRkUA8ovG9BmwxpeGH5e';


select * from events where anonymous_id = 'DCrvz9BLRkUA8ovG9BmwxpeGH5e' and event_name = 'checkout_completed';




select * from user_profile_final_v where anonymous_id = 'DC1Gr7DX9AQx81vtqkss8YTzHhU';
select * from user_profile where anonymous_id = 'DC1Gr7DX9AQx81vtqkss8YTzHhU';


select * from user_profile where anonymous_id = 'DC1O9AVv5kDE9BDpXWEvMEWBjWH';


select * from events where anonymous_id = 'DC1Gr7DX9AQx81vtqkss8YTzHhU' and event_name = 'checkout_completed';

['source.itemId','source.itemType','target.itemId','target.itemType','source.properties.pageInfo.pagePath','source.properties.pageInfo.pageName','source.properties.pageInfo.destinationURL','properties.cart_id','properties.currency','properties.shipping_address','properties.shipping_country','properties.shipping_zipcode','properties.payment_method','properties.shipping_state','properties.order_id','properties.shipping_address_new','device_model','device_name','device_brand','device_type','ip','os_name','connection_type']



select * from user_profile where anonymous_id = 'DC1O9AVv5kDE9BDpXWEvMEWBjWH';
select * from events where anonymous_id = 'DC1O9AVv5kDE9BDpXWEvMEWBjWH' and event_name = '';


--------fix audience size
select (users) from segment_users_final_v where segment_id = '1jXJ48BRyqzJ0EW4toUGrfumSZ2';
select length(users), at from segment_users where segment_id = '1jXJ48BRyqzJ0EW4toUGrfumSZ2' order by at desc;


-------fix total user by segment
select * from user_profile where anonymous_id = 'DC0aMjD9ByMPOtMhQA1h44R9CaM';
select * from user_profile_final_v where anonymous_id = 'DC0aMjD9ByMPOtMhQA1h44R9CaM';





SELECT
    pf.cate as key,
    count() AS value
FROM
(
    SELECT
        tenant_id,
        segment_id,
        users
    FROM
    (
        SELECT
            tenant_id,
            segment_id,
            argMaxMerge(users) AS users
        FROM segment_users_final
        WHERE tenant_id = 1 AND segment_id = '1jXaAW4eEtNMd4Cs5YPgr6dqJkq'
        GROUP BY
            tenant_id,
            segment_id
    )
    ARRAY JOIN users
) AS sf
INNER JOIN
(
    SELECT
        tenant_id,
        anonymous_id,
        str_vals[indexOf(str_keys, 'city')] AS cate
    FROM
    (
        SELECT
            tenant_id,
            anonymous_id,
            argMaxMerge(str_properties.keys) AS str_keys,
            argMaxMerge(str_properties.vals) AS str_vals
        FROM user_profile_final
        WHERE tenant_id = 1
        GROUP BY
            tenant_id,
            anonymous_id
    )
) AS pf ON (sf.tenant_id = pf.tenant_id) AND (sf.users = pf.anonymous_id)
WHERE cate != ''
GROUP BY
    tenant_id,
    segment_id,
    cate
ORDER BY
    value DESC,
    key ASC
;


select *
from (
      select *
      from segment_users_final_v
      where segment_id = '1jXaAW4eEtNMd4Cs5YPgr6dqJkq'
) array join users
order by  users;


select anonymous_id,
       arrayZip(str_properties.keys, str_properties.vals), at
from user_profile
where anonymous_id = 'DCavCaog5eLSGFwi8f8mBes25iU' order by at desc ;


select *
from user_profile
where anonymous_id = 'DCxiLJ08o0UHmx23jtcGWJ1hL3E'
order by at desc ;



[('lastVisit','2020-10-29T07:03:01Z'),('systems.mergeIdentifier','711'),('country','Pakistan'),('house_number','711'),('profile_image','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('acquisition_type','paid-google'),('first_name','My'),('gender','Female'),('device_os_version','6.91'),('last_name','Le'),('firstVisit','2020-10-28T05:06:44Z'),('dob','04/02/1987'),('device_model','CLK-Class'),('post_code','43567'),('phone_number','180 848 3248'),('loyalty_status','Gold'),('city','Yazman'),('device_platform','iOS'),('id','MyY8pXr'),('registration_start_date','27/05/2018'),('street','Hovde'),('timezone','Asia/Karachi'),('device_make','Mercedes-Benz'),('app_version','3.87'),('preferred_language','Italian'),('systems.lastUpdated','2020-10-29T07:03:55Z'),('appversion','3.87'),('previousVisit','2020-10-29T06:56:21Z'),('registrationstartdate','27/05/2018'),('profileimage','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jS4Rbl6dkv36aXQfWQaVHLxd5H_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jRzsvB5oj86EWtgMe0OSzgLboZ_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jP1RhZyXPnkRUsRGlfkUVAlLpD_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jSCYmsunY8PnDfEjd6iWP4fF5f_0_0StartReached','2020-10-28T07:07:28Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_0_0StartReached','2020-10-28T08:34:02Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0TargetReached','2020-10-28T07:07:39Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_1StartReached','2020-10-28T07:07:39Z'),('systems.goals.1jS6vp4IciwMvx2CucEwEAV07ZN_0_0StartReached','2020-10-28T07:30:36Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0TargetReached','2020-10-28T08:34:02Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0TargetReached','2020-10-29T04:08:05Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0TargetReached','2020-10-29T06:57:38Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0TargetReached','2020-10-29T07:04:16Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0TargetReached','2020-10-29T03:18:02Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_0_0StartReached','2020-10-29T04:08:26Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0TargetReached','2020-10-29T04:08:26Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0TargetReached','2020-10-29T06:57:38Z'),('properties.currency','VND'),('systems.goals.1jXchvHca1c6uiSpuACrUiRzrUJ_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXedSf8jHXH5tNapptC7arlz8s_0_0StartReached','2020-10-29T07:03:54Z')]
[('lastVisit','2020-10-29T07:03:01Z'),('systems.mergeIdentifier','711'),('country','Pakistan'),('house_number','711'),('profile_image','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('acquisition_type','paid-google'),('first_name','My'),('gender','Female'),('device_os_version','6.91'),('last_name','Le'),('firstVisit','2020-10-28T05:06:44Z'),('dob','04/02/1987'),('device_model','CLK-Class'),('post_code','43567'),('phone_number','180 848 3248'),('loyalty_status','Gold'),('city','Yazman'),('device_platform','iOS'),('id','MyY8pXr'),('registration_start_date','27/05/2018'),('street','Hovde'),('timezone','Asia/Karachi'),('device_make','Mercedes-Benz'),('app_version','3.87'),('preferred_language','Italian'),('systems.lastUpdated','2020-10-29T07:03:55Z'),('appversion','3.87'),('previousVisit','2020-10-29T06:56:21Z'),('registrationstartdate','27/05/2018'),('profileimage','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jS4Rbl6dkv36aXQfWQaVHLxd5H_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jRzsvB5oj86EWtgMe0OSzgLboZ_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jP1RhZyXPnkRUsRGlfkUVAlLpD_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jSCYmsunY8PnDfEjd6iWP4fF5f_0_0StartReached','2020-10-28T07:07:28Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_0_0StartReached','2020-10-28T08:34:02Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0TargetReached','2020-10-28T07:07:39Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_1StartReached','2020-10-28T07:07:39Z'),('systems.goals.1jS6vp4IciwMvx2CucEwEAV07ZN_0_0StartReached','2020-10-28T07:30:36Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0TargetReached','2020-10-28T08:34:02Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0TargetReached','2020-10-29T04:08:05Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0TargetReached','2020-10-29T06:57:38Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0TargetReached','2020-10-29T07:04:16Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0TargetReached','2020-10-29T03:18:02Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_0_0StartReached','2020-10-29T04:08:26Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0TargetReached','2020-10-29T04:08:26Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0TargetReached','2020-10-29T07:04:16Z'),('properties.currency','VND'),('systems.goals.1jXchvHca1c6uiSpuACrUiRzrUJ_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXedSf8jHXH5tNapptC7arlz8s_0_0StartReached','2020-10-29T07:03:54Z')]
[('lastVisit','2020-10-29T07:03:01Z'),('systems.mergeIdentifier','711'),('country','Pakistan'),('house_number','711'),('profile_image','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('acquisition_type','paid-google'),('first_name','My'),('gender','Female'),('device_os_version','6.91'),('last_name','Le'),('firstVisit','2020-10-28T05:06:44Z'),('dob','04/02/1987'),('device_model','CLK-Class'),('post_code','43567'),('phone_number','180 848 3248'),('loyalty_status','Gold'),('city','Yazman'),('device_platform','iOS'),('id','MyY8pXr'),('registration_start_date','27/05/2018'),('street','Hovde'),('timezone','Asia/Karachi'),('device_make','Mercedes-Benz'),('app_version','3.87'),('preferred_language','Italian'),('systems.lastUpdated','2020-10-29T07:03:55Z'),('appversion','3.87'),('previousVisit','2020-10-29T06:56:21Z'),('registrationstartdate','27/05/2018'),('profileimage','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jS4Rbl6dkv36aXQfWQaVHLxd5H_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jRzsvB5oj86EWtgMe0OSzgLboZ_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jP1RhZyXPnkRUsRGlfkUVAlLpD_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jSCYmsunY8PnDfEjd6iWP4fF5f_0_0StartReached','2020-10-28T07:07:28Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_0_0StartReached','2020-10-28T08:34:02Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0TargetReached','2020-10-28T07:07:39Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_1StartReached','2020-10-28T07:07:39Z'),('systems.goals.1jS6vp4IciwMvx2CucEwEAV07ZN_0_0StartReached','2020-10-28T07:30:36Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0TargetReached','2020-10-28T08:34:02Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0TargetReached','2020-10-29T04:08:05Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0TargetReached','2020-10-29T07:04:16Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0TargetReached','2020-10-29T07:04:16Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0TargetReached','2020-10-29T03:18:02Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_0_0StartReached','2020-10-29T04:08:26Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0TargetReached','2020-10-29T04:08:26Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0TargetReached','2020-10-29T07:04:16Z'),('properties.currency','VND'),('systems.goals.1jXchvHca1c6uiSpuACrUiRzrUJ_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXedSf8jHXH5tNapptC7arlz8s_0_0StartReached','2020-10-29T07:03:54Z')]
[('lastVisit','2020-10-29T07:03:01Z'),('systems.mergeIdentifier','711'),('country','Pakistan'),('house_number','711'),('profile_image','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('acquisition_type','paid-google'),('first_name','My'),('gender','Female'),('device_os_version','6.91'),('last_name','Le'),('firstVisit','2020-10-28T05:06:44Z'),('dob','04/02/1987'),('device_model','CLK-Class'),('post_code','43567'),('phone_number','180 848 3248'),('loyalty_status','Gold'),('city','Yazman'),('device_platform','iOS'),('id','MyY8pXr'),('registration_start_date','27/05/2018'),('street','Hovde'),('timezone','Asia/Karachi'),('device_make','Mercedes-Benz'),('app_version','3.87'),('preferred_language','Italian'),('systems.lastUpdated','2020-10-29T07:03:55Z'),('appversion','3.87'),('previousVisit','2020-10-29T06:56:21Z'),('registrationstartdate','27/05/2018'),('profileimage','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jS4Rbl6dkv36aXQfWQaVHLxd5H_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jRzsvB5oj86EWtgMe0OSzgLboZ_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jP1RhZyXPnkRUsRGlfkUVAlLpD_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jSCYmsunY8PnDfEjd6iWP4fF5f_0_0StartReached','2020-10-28T07:07:28Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_0_0StartReached','2020-10-28T08:34:02Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0TargetReached','2020-10-28T07:07:39Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_1StartReached','2020-10-28T07:07:39Z'),('systems.goals.1jS6vp4IciwMvx2CucEwEAV07ZN_0_0StartReached','2020-10-28T07:30:36Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0TargetReached','2020-10-28T08:34:02Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0TargetReached','2020-10-29T04:08:05Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0TargetReached','2020-10-29T07:04:16Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0TargetReached','2020-10-29T07:04:16Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0TargetReached','2020-10-29T03:18:02Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_0_0StartReached','2020-10-29T04:08:26Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0TargetReached','2020-10-29T04:08:26Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0TargetReached','2020-10-29T07:04:16Z'),('properties.currency','VND'),('systems.goals.1jXchvHca1c6uiSpuACrUiRzrUJ_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXedSf8jHXH5tNapptC7arlz8s_0_0StartReached','2020-10-29T07:03:54Z')]
[('lastVisit','2020-10-29T07:03:01Z'),('systems.mergeIdentifier','711'),('country','Pakistan'),('house_number','711'),('profile_image','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('acquisition_type','paid-google'),('first_name','My'),('gender','Female'),('device_os_version','6.91'),('last_name','Le'),('firstVisit','2020-10-28T05:06:44Z'),('dob','04/02/1987'),('device_model','CLK-Class'),('post_code','43567'),('phone_number','180 848 3248'),('loyalty_status','Gold'),('city','Yazman'),('device_platform','iOS'),('id','MyY8pXr'),('registration_start_date','27/05/2018'),('street','Hovde'),('timezone','Asia/Karachi'),('device_make','Mercedes-Benz'),('app_version','3.87'),('preferred_language','Italian'),('systems.lastUpdated','2020-10-29T07:03:55Z'),('appversion','3.87'),('previousVisit','2020-10-29T06:56:21Z'),('registrationstartdate','27/05/2018'),('profileimage','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jS4Rbl6dkv36aXQfWQaVHLxd5H_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jRzsvB5oj86EWtgMe0OSzgLboZ_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jP1RhZyXPnkRUsRGlfkUVAlLpD_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jSCYmsunY8PnDfEjd6iWP4fF5f_0_0StartReached','2020-10-28T07:07:28Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_0_0StartReached','2020-10-28T08:34:02Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0TargetReached','2020-10-28T07:07:39Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_1StartReached','2020-10-28T07:07:39Z'),('systems.goals.1jS6vp4IciwMvx2CucEwEAV07ZN_0_0StartReached','2020-10-28T07:30:36Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0TargetReached','2020-10-28T08:34:02Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0TargetReached','2020-10-29T04:08:05Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0TargetReached','2020-10-29T07:04:16Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0TargetReached','2020-10-29T07:04:16Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0TargetReached','2020-10-29T03:18:02Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_0_0StartReached','2020-10-29T04:08:26Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0TargetReached','2020-10-29T04:08:26Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0TargetReached','2020-10-29T07:04:16Z'),('properties.currency','VND'),('systems.goals.1jXchvHca1c6uiSpuACrUiRzrUJ_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXedSf8jHXH5tNapptC7arlz8s_0_0StartReached','2020-10-29T07:03:54Z')]
[('lastVisit','2020-10-29T07:03:01Z'),('systems.mergeIdentifier','711'),('country','Pakistan'),('house_number','711'),('profile_image','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('acquisition_type','paid-google'),('first_name','My'),('gender','Female'),('device_os_version','6.91'),('last_name','Le'),('firstVisit','2020-10-28T05:06:44Z'),('dob','04/02/1987'),('device_model','CLK-Class'),('post_code','43567'),('phone_number','180 848 3248'),('loyalty_status','Gold'),('city','Yazman'),('device_platform','iOS'),('id','MyY8pXr'),('registration_start_date','27/05/2018'),('street','Hovde'),('timezone','Asia/Karachi'),('device_make','Mercedes-Benz'),('app_version','3.87'),('preferred_language','Italian'),('systems.lastUpdated','2020-10-29T07:03:55Z'),('appversion','3.87'),('previousVisit','2020-10-29T06:56:21Z'),('registrationstartdate','27/05/2018'),('profileimage','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jS4Rbl6dkv36aXQfWQaVHLxd5H_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jRzsvB5oj86EWtgMe0OSzgLboZ_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jP1RhZyXPnkRUsRGlfkUVAlLpD_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jSCYmsunY8PnDfEjd6iWP4fF5f_0_0StartReached','2020-10-28T07:07:28Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_0_0StartReached','2020-10-28T08:34:02Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0TargetReached','2020-10-28T07:07:39Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_1StartReached','2020-10-28T07:07:39Z'),('systems.goals.1jS6vp4IciwMvx2CucEwEAV07ZN_0_0StartReached','2020-10-28T07:30:36Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0TargetReached','2020-10-28T08:34:02Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0TargetReached','2020-10-29T04:08:05Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0TargetReached','2020-10-29T06:57:38Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0TargetReached','2020-10-29T07:04:16Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0TargetReached','2020-10-29T03:18:02Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_0_0StartReached','2020-10-29T04:08:26Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0TargetReached','2020-10-29T04:08:26Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0TargetReached','2020-10-29T07:04:16Z'),('properties.currency','VND'),('systems.goals.1jXchvHca1c6uiSpuACrUiRzrUJ_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXedSf8jHXH5tNapptC7arlz8s_0_0StartReached','2020-10-29T07:03:54Z')]
[('lastVisit','2020-10-29T07:03:01Z'),('systems.mergeIdentifier','711'),('country','Pakistan'),('house_number','711'),('profile_image','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('acquisition_type','paid-google'),('first_name','My'),('gender','Female'),('device_os_version','6.91'),('last_name','Le'),('firstVisit','2020-10-28T05:06:44Z'),('dob','04/02/1987'),('device_model','CLK-Class'),('post_code','43567'),('phone_number','180 848 3248'),('loyalty_status','Gold'),('city','Yazman'),('device_platform','iOS'),('id','MyY8pXr'),('registration_start_date','27/05/2018'),('street','Hovde'),('timezone','Asia/Karachi'),('device_make','Mercedes-Benz'),('app_version','3.87'),('preferred_language','Italian'),('systems.lastUpdated','2020-10-29T07:03:55Z'),('appversion','3.87'),('previousVisit','2020-10-29T06:56:21Z'),('registrationstartdate','27/05/2018'),('profileimage','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jS4Rbl6dkv36aXQfWQaVHLxd5H_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jRzsvB5oj86EWtgMe0OSzgLboZ_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jP1RhZyXPnkRUsRGlfkUVAlLpD_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jSCYmsunY8PnDfEjd6iWP4fF5f_0_0StartReached','2020-10-28T07:07:28Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_0_0StartReached','2020-10-28T08:34:02Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0TargetReached','2020-10-28T07:07:39Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_1StartReached','2020-10-28T07:07:39Z'),('systems.goals.1jS6vp4IciwMvx2CucEwEAV07ZN_0_0StartReached','2020-10-28T07:30:36Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0TargetReached','2020-10-28T08:34:02Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0TargetReached','2020-10-29T04:08:05Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0TargetReached','2020-10-29T06:57:38Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0TargetReached','2020-10-29T07:04:16Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0TargetReached','2020-10-29T03:18:02Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_0_0StartReached','2020-10-29T04:08:26Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0TargetReached','2020-10-29T04:08:26Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0TargetReached','2020-10-29T07:04:16Z'),('properties.currency','VND'),('systems.goals.1jXchvHca1c6uiSpuACrUiRzrUJ_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXedSf8jHXH5tNapptC7arlz8s_0_0StartReached','2020-10-29T07:03:54Z')]
[('lastVisit','2020-10-29T07:03:01Z'),('systems.mergeIdentifier','711'),('country','Pakistan'),('house_number','711'),('profile_image','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('acquisition_type','paid-google'),('first_name','My'),('gender','Female'),('device_os_version','6.91'),('last_name','Le'),('firstVisit','2020-10-28T05:06:44Z'),('dob','04/02/1987'),('device_model','CLK-Class'),('post_code','43567'),('phone_number','180 848 3248'),('loyalty_status','Gold'),('city','Yazman'),('device_platform','iOS'),('id','MyY8pXr'),('registration_start_date','27/05/2018'),('street','Hovde'),('timezone','Asia/Karachi'),('device_make','Mercedes-Benz'),('app_version','3.87'),('preferred_language','Italian'),('systems.lastUpdated','2020-10-29T07:03:55Z'),('appversion','3.87'),('previousVisit','2020-10-29T06:56:21Z'),('registrationstartdate','27/05/2018'),('profileimage','https://robohash.org/rationeconsequaturaut.jpg?size=100x100&set=set1'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jS4Rbl6dkv36aXQfWQaVHLxd5H_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jRzsvB5oj86EWtgMe0OSzgLboZ_0_0StartReached','2020-10-29T02:06:21Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jP1RhZyXPnkRUsRGlfkUVAlLpD_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jSCYmsunY8PnDfEjd6iWP4fF5f_0_0StartReached','2020-10-28T07:07:28Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_0_0StartReached','2020-10-28T08:34:02Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_0TargetReached','2020-10-28T07:07:39Z'),('systems.goals.1jRwUvE95fvIoguBMEgIC21OI65_0_1StartReached','2020-10-28T07:07:39Z'),('systems.goals.1jS6vp4IciwMvx2CucEwEAV07ZN_0_0StartReached','2020-10-28T07:30:36Z'),('systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0TargetReached','2020-10-28T08:34:02Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0StartReached','2020-10-29T02:27:38Z'),('systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0TargetReached','2020-10-29T02:06:21Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0TargetReached','2020-10-29T04:08:05Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0StartReached','2020-10-29T02:50:42Z'),('systems.goals.1jUUf0QX4lcEl8LRtTeLAA5mH28_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPKiEG8X2GZ3tIUH8WGEia38l0_1_0TargetReached','2020-10-29T06:57:38Z'),('systems.goals.1jX8v2CCeTRxcA5T61FhJf0pevr_0_0TargetReached','2020-10-29T02:51:16Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_1_0TargetReached','2020-10-29T07:04:16Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0TargetReached','2020-10-29T03:18:02Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0StartReached','2020-10-29T04:08:04Z'),('systems.goals.1jPN77SZbyFj2aYoeirVxGGcKJD_0_0StartReached','2020-10-29T04:08:26Z'),('systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0TargetReached','2020-10-29T04:08:26Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXaAW4eEtNMd4Cs5YPgr6dqJkq_0_0TargetReached','2020-10-29T06:57:38Z'),('properties.currency','VND'),('systems.goals.1jXchvHca1c6uiSpuACrUiRzrUJ_0_0StartReached','2020-10-29T07:03:54Z'),('systems.goals.1jXedSf8jHXH5tNapptC7arlz8s_0_0StartReached','2020-10-29T07:03:54Z')]


show create table user_profile;


------------------test audience size
select * from segment_users where segment_id = '1jimK7w6tAuunXM2vsqlLbj4U57' order by at desc ;


show create table rfm_metrics;

SELECT  tenant_id,
        anonymous_id,
        recency,
        frequency1,
        frequency30,
        frequency60,
        frequency90,
        monetary30,
        monetary60,
        monetary90,
        monetary1
FROM rfm_metrics
WHERE date = '2020-11-1';











SELECT
    pf.cate as key,
    count() AS value
FROM
(
    SELECT
        tenant_id,
        segment_id,
        users
    FROM
    (
        SELECT
            tenant_id,
            segment_id,
            argMaxMerge(users) AS users
        FROM segment_users_final
        WHERE tenant_id = 1 AND segment_id = '1jjAr7I8uuCL3i4JBPMPe6mg91X'
        GROUP BY
            tenant_id,
            segment_id
    )
    ARRAY JOIN users
) AS sf
INNER JOIN
(
    SELECT
        tenant_id,
        anonymous_id,
        str_vals[indexOf(str_keys, 'city')] AS cate
    FROM
    (
        SELECT
            tenant_id,
            anonymous_id,
            argMaxMerge(str_properties.keys) AS str_keys,
            argMaxMerge(str_properties.vals) AS str_vals
        FROM user_profile_final
        WHERE tenant_id = 1
        GROUP BY
            tenant_id,
            anonymous_id
    )
) AS pf ON (sf.tenant_id = pf.tenant_id) AND (sf.users = pf.anonymous_id)
WHERE cate != ''
GROUP BY
    tenant_id,
    segment_id,
    cate
ORDER BY
    value DESC,
    key ASC
;


-- Ko nhan user in segment anylytics

select * from segment_users where segment_id = '1jjAr7I8uuCL3i4JBPMPe6mg91X';
['DC179BzFq4FImX09CIai9CS9C9B','DC19A9CYC0tOI4ygtIN14u2VsvN']

select * from user_profile where anonymous_id = 'DC179BzFq4FImX09CIai9CS9C9B' order by at desc ;

select * from user_profile where anonymous_id = 'DC19A9CYC0tOI4ygtIN14u2VsvN' order by at desc ;


------------------------
['lastVisit','loyalty_status','device_platform','gender','device_os_version','firstVisit','timezone','preferred_language','country','device_make','street','id','acquisition_type','app_version','city','first_name','last_name','profile_image','post_code','phone_number','dob','device_model','registration_start_date','house_number','systems.lastUpdated','systems.goals.1jUWfJ2owtquABjbDWxQP6oxnkU_0_0StartReached','systems.goals.1jVMk40iodyvOukinjcN9BeSSSk_0_0StartReached','systems.goals.1jgrwemEq2hFvxTx0Xkjx9ELgkD_0_0StartReached','systems.goals.1jimK7w6tAuunXM2vsqlLbj4U57_0_0StartReached','systems.goals.1jjAr7I8uuCL3i4JBPMPe6mg91X_0_0StartReached','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0StartReached','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_1StartReached','systems.goals.1jjAr7I8uuCL3i4JBPMPe6mg91X_0_0TargetReached','systems.goals.1jjAr7I8uuCL3i4JBPMPe6mg91X_0_1StartReached','systems.goals.1jimK7w6tAuunXM2vsqlLbj4U57_0_0TargetReached','systems.goals.1jXJ48BRyqzJ0EW4toUGrfumSZ2_0_0StartReached','systems.goals.1jXsqzMx9owAGqxqZI5SUoGsI7r_0_0StartReached','systems.goals.1jSO0zGZGoga6lX7vpqwBichCy5_0_0StartReached','systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0StartReached','systems.goals.1jXCpts2E5OPTIx9654kaml2qhj_0_0StartReached','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0StartReached','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_1_0TargetReached','systems.goals.1jXsqzMx9owAGqxqZI5SUoGsI7r_0_0TargetReached','systems.goals.1jjAr7I8uuCL3i4JBPMPe6mg91X_0_1TargetReached','systems.goals.1jVgRUc7cIsyIsRs0gzXkWhl15U_0_0TargetReached','systems.goals.1jPFD35Kw29c0uBl9JttH9ofXhp_0_0TargetReached']


--------------------------------------------
alter table user_profile modify column at
;




select * from user_profile where anonymous_id = 'DC2Za9AboCeQHt9BA3XF9Chg3KY';



select toUnixTimestamp(now()) ;

SELECT
    ['{MAX}', '{AVG}', '{MED}', '{MIN}'] AS {METRIC_KEY},
    [toFloat64(maxOrDefault(days)), avgOrDefault(days), medianOrDefault(days), toFloat64(minOrDefault(days))] AS {METRIC_VALUE}
FROM
(
    SELECT
        sf.tenant_id,
        sf.segment_id,
        round((toUnixTimestamp(now()) - t) / ((24 * 60) * 60)) AS days
    FROM
    (
        SELECT
            tenant_id,
            segment_id,
            users
        FROM
        (
            SELECT
                tenant_id,
                segment_id,
                argMaxMerge(users) AS users
            FROM segment_users_final
            WHERE tenant_id = 1 AND segment_id = '1jjAr7I8uuCL3i4JBPMPe6mg91X'
            GROUP BY
                tenant_id,
                segment_id
        )
        ARRAY JOIN users
    ) AS sf
    INNER JOIN
    (
        SELECT
            tenant_id,
            anonymous_id,
            num_vals[indexOf(num_keys, 'last_order_at')] AS t
        FROM
        (
            SELECT
                tenant_id,
                anonymous_id,
                argMaxMerge(num_properties.keys) AS num_keys,
                argMaxMerge(num_properties.vals) AS num_vals
            FROM user_profile_final
            WHERE tenant_id = 1
            GROUP BY
                     tenant_id,
                     anonymous_id
        )
    ) AS pf ON (sf.users = pf.anonymous_id) AND (sf.tenant_id = pf.tenant_id)
    WHERE t > 0
);


-------- Update last order at

select *
    from (
          SELECT tenant_id,
                 anonymous_id,
                 num_vals[indexOf(num_keys, 'last_order_at')] AS t,
                 length(toString(t)) as length
          FROM (
                SELECT tenant_id,
                       anonymous_id,
                       argMaxMerge(num_properties.keys) AS num_keys,
                       argMaxMerge(num_properties.vals) AS num_vals
                FROM user_profile_final
                WHERE tenant_id = 1
                GROUP BY tenant_id,
                         anonymous_id
                   )
             ) where length > 10;

alter table user_profile delete where anonymous_id = 'DC2jcXQnSJhAr2rOA7c6v4ykT8E';
alter table user_profile_final delete where anonymous_id = 'DC2jcXQnSJhAr2rOA7c6v4ykT8E';