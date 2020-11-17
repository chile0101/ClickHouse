B1: faust -A prile.workflows.conversion_broadcast.app worker -l info -p 6068
B2: create events, user_profile
B3:

faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":2, "anonymous_id": "a1", "event_name": "View", "identity_keys":["user_id"], "identity_vals" : ["1"], "str_properties_keys":["gender"],"str_properties_vals":["male"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":[""],"arr_properties_vals":[[]],"at":1598085203}'




faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "View Product", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[12],"arr_properties_keys":[""],"arr_properties_vals":[[]],"at":1598085203}'



faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "View Product", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[1],"arr_properties_keys":[""],"arr_properties_vals":[[]],"at":1598931880}'

faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "View Product", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[1],"arr_properties_keys":[""],"arr_properties_vals":[[]],"at":1598931880}'

faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "View Product", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[1],"arr_properties_keys":[""],"arr_properties_vals":[[]],"at":1598931980}'


faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "View Product", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[1],"arr_properties_keys":[""],"arr_properties_vals":[[]],"at":1598931980}'


faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "View Product", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[1],"arr_properties_keys":[""],"arr_properties_vals":[[]],"at":1598931980}'


insert into user_profile values
('a1' , 1, ['email'],['chi@'], ['gender'],['male'],[],[],[],[], now())
;




select * from user_profile where tenant_id = 100 and anonymous_id = 'a1';

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




select * from user_profile_final_v where tenant_id = 25 and anonymous_id = 'a3';
-- test xem update ok ko
faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":25, "anonymous_id": "a1", "event_name": "Buy IPHONE", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[3],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600931080}'


faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":25, "anonymous_id": "a2", "identity_keys":["user_id", "email"], "identity_vals" : ["user_1","test@gmail.com"], "str_properties_keys":["gender","location_city"],"str_properties_vals":["male","Ho Chi Minh"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600931080}'
faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":25, "anonymous_id": "a2", "event_name": "Buy IPHONE", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[3],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600931083}'
faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":25, "anonymous_id": "a2", "event_name": "Buy IPHONE", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":[],"str_properties_vals":[],"num_properties_keys":["total_value"],"num_properties_vals":[3],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600931085}'


faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":25, "anonymous_id": "a2", "event_name": "Buy IPHONE", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[3],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600931086}'

faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":25, "anonymous_id": "a2", "event_name": "Buy IPHONE", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[4],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600931087}'


--- a3
-- first time not correct
faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":25, "anonymous_id": "a3", "identity_keys":["user_id", "email"], "identity_vals" : ["user_1","test@gmail.com"], "str_properties_keys":["gender","location_city"],"str_properties_vals":["male","Ho Chi Minh"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600931090}'
faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":25, "anonymous_id": "a3", "event_name": "Buy IPHONE", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[4],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600931087}'

--- a4
-- first time not correct
select * from user_profile where tenant_id = 25 and anonymous_id = 'a4';
select * from user_profile_final_v where tenant_id = 25 and anonymous_id = 'a4';
faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":25, "anonymous_id": "a4", "identity_keys":["user_id", "email"], "identity_vals" : ["user_1","test@gmail.com"], "str_properties_keys":["gender","location_city"],"str_properties_vals":["male","Ho Chi Minh"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600931090}'
faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":25, "anonymous_id": "a4", "event_name": "Buy IPHONE", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[4],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600931087}'

faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":25, "anonymous_id": "a4", "event_name": "Buy IPHONE", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[5],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600942370}'

faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":25, "anonymous_id": "a4", "event_name": "Buy IPHONE", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[5],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600942372}'


select FROM_UNIXTIME(1600931092)
select toUnixTimestamp(toDateTime('2020-09-24 10:12:46'))


----- a5
select now();
select toUnixTimestamp(now());
select FROM_UNIXTIME(1600943435);
select * from user_profile where tenant_id = 25 and anonymous_id = 'a5';
select * from user_profile_final_v where tenant_id = 25 and anonymous_id = 'a5';
faust -A prile.workflows.profile_update.app send "profile-updated"  '{"tenant_id":25, "anonymous_id": "a5", "identity_keys":["user_id", "email"], "identity_vals" : ["user_1","test@gmail.com"], "str_properties_keys":["gender","location_city"],"str_properties_vals":["male","Ho Chi Minh"],"num_properties_keys":[],"num_properties_vals":[],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600931090}'
faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":25, "anonymous_id": "a5", "event_name": "Buy IPHONE", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[5],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600943430}'

faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":25, "anonymous_id": "a5", "event_name": "Buy IPHONE", "identity_keys":["user_id"], "identity_vals" : ["levanchi"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[5],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1600943435}'


-----------------------------TEST_AGAIN
select FROM_UNIXTIME(1601349660);
select * from user_profile where tenant_id = 1 and anonymous_id = 'a1';
select * from user_profile_final_v where tenant_id = 1 and anonymous_id = 'a1';
faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "Buy Iphone", "identity_keys":["user_id"], "identity_vals" : ["doi_ko"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[5000],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1601349660}'
a1,1,"['user_id','email','facebook_id']","['user_1','chile74@gmail.com','KHK23']","['gender','city','currency']","['Male','Ho Chi Minh','VND']","['total_order','last_order_at','avg_order_value','revenue']","[1,1601349660,5000,5000]",['channels'],"[['google','mailchim']]",2020-09-29 03:21:00


faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "Buy Iphone 7", "identity_keys":["user_id"], "identity_vals" : ["doi_ko"], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[12000],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1601351692}'
a1,1,"['user_id','email','facebook_id']","['user_1','chile74@gmail.com','KHK23']","['gender','city','currency']","['Male','Ho Chi Minh','VND']","['total_order','last_order_at','avg_order_value','revenue']","[2,1601351692,8500,17000]",['channels'],"[['google','mailchim']]",2020-09-29 03:54:52

faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a2", "event_name": "Buy Xiaomi 7", "identity_keys":["user_id"], "identity_vals" : ["doi_ko"], "str_properties_keys":["currency"],"str_properties_vals":["USD"],"num_properties_keys":["total_value"],"num_properties_vals":[10],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1601354923}'

faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "Buy Xiaomi 7", "identity_keys":["user_id"], "identity_vals" : ["doi_ko"], "str_properties_keys":["currency"],"str_properties_vals":["USD"],"num_properties_keys":["total_value"],"num_properties_vals":[1],"arr_properties_keys":["arr_test"],"arr_properties_vals":[["val1","val2"]],"at":1601354926}'






faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a1", "event_name": "Buy Iphone 11", "identity_keys":[], "identity_vals" : [], "str_properties_keys":["currency"],"str_properties_vals":["USD"],"num_properties_keys":["total_value"],"num_properties_vals":[2],"arr_properties_keys":[],"arr_properties_vals":[],"at":1601354940}'
 Input Conversion Event: <ConversionEvent: tenant_id=1, anonymous_id='a1', event_name='Buy Iphone 11', identity_keys=[], identity_vals=[], str_properties_keys=['currency'], str_properties_vals=['USD'], num_properties_keys=['total_value'], num_properties_vals=[2], arr_properties_keys=[], arr_properties_vals=[], at=1601354935>


faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a3", "event_name": "Buy Xiaomi 7", "identity_keys":[], "identity_vals" : [], "str_properties_keys":["currency"],"str_properties_vals":["USD"],"num_properties_keys":["total_value"],"num_properties_vals":[2],"arr_properties_keys":[],"arr_properties_vals":[],"at":1601354923}'


faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"tenant_id":1, "anonymous_id": "a3", "event_name": "Buy Xiaomi 7", "identity_keys":[], "identity_vals" : [], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[64000],"arr_properties_keys":[],"arr_properties_vals":[],"at":1601356620}'



faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"id":"event-123","tenant_id":1, "anonymous_id": "a1", "event_name": "Buy Xiaomi 123", "identity_keys":[], "identity_vals" : [], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[64000],"arr_properties_keys":[],"arr_properties_vals":[],"at":1601542585}'


select * from user_profile_final_v where tenant_id = 1 and anonymous_id = 'a1';

----- TEST ON SERVER
select toUnixTimestamp(now());
select * from user_profile_final where tenant_id = 1 and anonymous_id = 'a1';
select * from user_profile_final_v where tenant_id = 1 and  anonymous_id = 'a1';

faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"id":"event-123","tenant_id":1, "anonymous_id": "a1", "event_name": "Buy Xiaomi 123", "identity_keys":[], "identity_vals" : [], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[64000],"arr_properties_keys":[],"arr_properties_vals":[],"at":1601543496}'

faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"id":"event-123","tenant_id":1, "anonymous_id": "a1", "event_name": "Buy Xiaomi 123", "identity_keys":[], "identity_vals" : [], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[64000],"arr_properties_keys":[],"arr_properties_vals":[],"at":1601543691}'


faust -A prile.workflows.conversion_broadcast.app send "conversion-event"  '{"id":"event-123","tenant_id":1, "anonymous_id": "a1", "event_name": "Buy Xiaomi 123", "identity_keys":[], "identity_vals" : [], "str_properties_keys":["currency"],"str_properties_vals":["VND"],"num_properties_keys":["total_value"],"num_properties_vals":[64000],"arr_properties_keys":[],"arr_properties_vals":[],"at":1601550212}'



select count() from user_profile;

select * from user_profile
where anonymous_id = 'DC2jcXQnSJhAr2rOA7c6v4ykT8E'
order by at desc ;



faust -A prile.workflows.conversion_broadcast.app send "conversion-events" '{"eventType":"checkout_completed","itemId":"DC3T77N4k3qLhQ03V10OExVEKFQ","itemType":"event","persistent":true,"profileId":"DC2jcXQnSJhAr2rOA7c6v4ykT8E","properties":{"cart_id":"1","cart_subtotal_value":0,"country":"VI","coupon_added":false,"currency":"vnd","discount_value":0,"gender":"male","order_id":"1","payment_method":"COD","shipping_address":"vi","shipping_cost":0,"shipping_country":"VI","shipping_state":"vi","shipping_zipcode":"70000","total_value":20},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","sendAt":1604329967888,"sessionId":"ae4e95bb-a4ed-0767-32cf-2a2c58aa91b4","source":{"itemId":"https://weburnit.github.io/","itemType":"page","properties":{"attributes":[],"consentTypes":[],"interests":{},"pageInfo":{"destinationURL":"https://weburnit.github.io/","pageName":"Pullman Hotel Resorts - Saloniz Solution","pagePath":"/","referringURL":""}},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","version":null},"target":{"itemId":"/","itemType":"page","properties":{},"scope":"JS-1iRNWBw2hNQTRQibnJeb8NTep7u","version":null},"tenant_id":1,"timeStamp":1604329967869,"version":null}'



select * from user_profile_final_v where anonymous_id = 'DC5oh613LB8E5OXVs73H8OO9A7k';
select * from user_profile where anonymous_id = 'DC5oh613LB8E5OXVs73H8OO9A7k' order by at desc ;
alter table user_profile delete where anonymous_id = 'DC539BRX9CYcvtkgMnGINv6LcaB';
alter table user_profile_final delete where anonymous_id = 'DC539BRX9CYcvtkgMnGINv6LcaB';



select * from user_profile where anonymous_id = 'DC539BRX9CYcvtkgMnGINv6LcaB' order by at desc ;

select * from user_profile_final_v where anonymous_id = 'DC539BRX9CYcvtkgMnGINv6LcaB' ;
select * from user_profile;


select fromUnixTimestamp64Milli(1604331595254);
select now64() as time, toTypeName(time) as type;
select toUnixTimestamp64Milli(toDateTime64('2020-11-03 08:28:12.158', 3, 'UTC')); --1604392092158
