-- id          varchar                  not null
--         constraint segments_pkey
--             primary key,
--     name        varchar                  not null,
--     description varchar                  not null,
--     system      boolean                  not null,
--     status      varchar,
--     opt_out     jsonb,
--     conditions  jsonb                    not null,
--     created_at  timestamp with time zone not null,
--     updated_at  timestamp with time zone not null,
--     tenant_id   bigint;
show create table segment_user;
CREATE TABLE segment_user_preview
(
    `segment_id` String,
    `tenant_id` UInt16,
    `user` String,
    `status` UInt8,
    `at` DateTime64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(at)
ORDER BY (tenant_id, segment_id, user, at)
;


show tables;
show create table profile_str_final;

drop table segments;
create table segments
(    tenant_id   UInt16,
    id          String,
    name        String,
    description String,
    type        String, -- live or historical
    status      String,
    conditions  String,
    mark_as_deleted Int8,
    created_at  DateTime64 ,
    updated_at  DateTime64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (tenant_id, id, created_at);

drop table segments_v;
create view segments_v
as
select tenant_id,
       id as segment_id,
       argMax(name, updated_at) as name,
       argMax(description, updated_at) as description,
       argMax(type, updated_at) as type,
       argMax(status, updated_at) as status,
       argMax(conditions, updated_at) as conditions,
       argMax(mark_as_deleted, updated_at) as mark_as_deleted,
       argMax(created_at, updated_at) as created_time,
       max(updated_at) as updated_time
from segments
group by tenant_id, id;



INSERT INTO segments (tenant_id, id, name, description, type, status, conditions, mark_as_deleted, created_at, updated_at)
VALUES (1, '1mwzjcwRMp8fpVjfUNHFun7wuRr', '12_01_2021_checkout_1_time_p03', 'segment single', 0,'live', 'published', '[{"type": "and", "isParent": true, "parentId": "root", "childrenAsList": [{"type": "and", "mainData": [{"id": "kjo4bxe0_46884200069683235", "step": [{"id": "kjo4bxe0_46884200069683235", "nodeType": "eventType", "operator": "greaterThan", "nodeTypeValue": "checkout_completed_(r)", "operatorValue": "1"}, {"id": "kjo4bxe0_2810139865001391", "nodeType": "windowType", "operator": null, "nodeTypeValue": "within", "operatorValue": 5}]}], "parentId": "kjo4bs83_10546329384505548", "conditionValue": "generic_funnel"}], "conditionValue": "booleanCondition"}]', 1, '2021-01-12 02:46:17.659036', '2021-01-12 02:46:17.659036');
INSERT INTO segments (tenant_id, id, name, description, system, type, status, conditions, mark_as_deleted, created_at, updated_at) VALUES (1, '1mwzjshD5jnmmgIE6vAqM1pPWMD', '12_01_2021_product_1_time_sn_p03', '12_01_2021_product_1_time_sn_p03', 0,'historical', 'published', '[{"type": "and", "isParent": true, "parentId": "root", "childrenAsList": [{"type": "and", "mainData": [{"id": "kjs19lq6_46931906582943594", "step": [{"id": "kjs19lq6_46931906582943594", "nodeType": "eventType", "operator": "greaterThan", "nodeTypeValue": "product_cart_added_(r)", "operatorValue": 1}, {"id": "kjs19lq6_785236355393115", "nodeType": "windowType", "operator": null, "nodeTypeValue": "within", "operatorValue": 5}]}], "parentId": "kjs19hje_12647204403674883", "conditionValue": "generic_funnel"}, {"type": "and", "mainData": [{"id": "kjs19qbj_17091245710758463", "nodeType": "eventType", "nodeTypeValue": "1mwzjcwRMp8fpVjfUNHFun7wuRr", "operatorValue": "true"}], "parentId": "kjs19hje_12647204403674883", "conditionValue": "segment"}], "conditionValue": "booleanCondition"}]', 1,'2021-01-12 02:46:19.138497', '2021-01-12 02:46:19.138497');
INSERT INTO segments (tenant_id, id, name, description, system, type, status, conditions, mark_as_deleted, created_at, updated_at) VALUES (1, '1mwzv7n9Wq1p1h56bcmhthVD95q', '12_01_2021_checkout_1_time_p00', 'segment single', 0, 'live','published', '[{"type": "and", "isParent": true, "parentId": "root", "childrenAsList": [{"type": "and", "mainData": [{"id": "kjo4bxe0_46884200069683235", "step": [{"id": "kjo4bxe0_46884200069683235", "nodeType": "eventType", "operator": "greaterThan", "nodeTypeValue": "checkout_completed_(r)", "operatorValue": "1"}, {"id": "kjo4bxe0_2810139865001391", "nodeType": "windowType", "operator": null, "nodeTypeValue": "within", "operatorValue": 5}]}], "parentId": "kjo4bs83_10546329384505548", "conditionValue": "generic_funnel"}], "conditionValue": "booleanCondition"}]', 1,'2021-01-12 02:47:48.759418', '2021-01-12 02:47:48.759419');
INSERT INTO segments (tenant_id, id, name, description, system, type, status, conditions, mark_as_deleted, created_at, updated_at) VALUES (1, '1mwzvE8UftLjEtIrTQEDZx81PNQ', '12_01_2021_product_1_time_sn_p00', '12_01_2021_product_1_time_sn_p00', 0, 'live','published', '[{"type": "and", "isParent": true, "parentId": "root", "childrenAsList": [{"type": "and", "mainData": [{"id": "kjs19lq6_46931906582943594", "step": [{"id": "kjs19lq6_46931906582943594", "nodeType": "eventType", "operator": "greaterThan", "nodeTypeValue": "product_cart_added_(r)", "operatorValue": 1}, {"id": "kjs19lq6_785236355393115", "nodeType": "windowType", "operator": null, "nodeTypeValue": "within", "operatorValue": 5}]}], "parentId": "kjs19hje_12647204403674883", "conditionValue": "generic_funnel"}, {"type": "and", "mainData": [{"id": "kjs19qbj_17091245710758463", "nodeType": "eventType", "nodeTypeValue": "1mwzv7n9Wq1p1h56bcmhthVD95q", "operatorValue": "true"}], "parentId": "kjs19hje_12647204403674883", "conditionValue": "segment"}], "conditionValue": "booleanCondition"}]', 1,'2021-01-12 02:47:49.486891', '2021-01-12 02:47:49.486892');
INSERT INTO segments (tenant_id, id, name, description, system, type, status, conditions, mark_as_deleted, created_at, updated_at) VALUES (1, '1mx6FkMoG0R8k9zgC5qDxfVagmW', '12_01_2021_checkout_1_time_p01', 'segment single', 0, 'live','published', '[{"type": "and", "isParent": true, "parentId": "root", "childrenAsList": [{"type": "and", "mainData": [{"id": "kjo4bxe0_46884200069683235", "step": [{"id": "kjo4bxe0_46884200069683235", "nodeType": "eventType", "operator": "greaterThan", "nodeTypeValue": "checkout_completed_(r)", "operatorValue": "1"}, {"id": "kjo4bxe0_2810139865001391", "nodeType": "windowType", "operator": null, "nodeTypeValue": "within", "operatorValue": 5}]}], "parentId": "kjo4bs83_10546329384505548", "conditionValue": "generic_funnel"}], "conditionValue": "booleanCondition"}]', 1,'2021-01-12 03:39:52.211402', '2021-01-12 03:39:52.211402');
INSERT INTO segments (tenant_id, id, name, description, system, type, status, conditions, mark_as_deleted, created_at, updated_at) VALUES (1, '1mx6FmQkdUHhIgYU7BMi6kzxgv1', '12_01_2021_product_1_time_sn_p01', '12_01_2021_product_1_time_sn_p01', 0,'live', 'published', '[{"type": "and", "isParent": true, "parentId": "root", "childrenAsList": [{"type": "and", "mainData": [{"id": "kjs19lq6_46931906582943594", "step": [{"id": "kjs19lq6_46931906582943594", "nodeType": "eventType", "operator": "greaterThan", "nodeTypeValue": "product_cart_added_(r)", "operatorValue": 1}, {"id": "kjs19lq6_785236355393115", "nodeType": "windowType", "operator": null, "nodeTypeValue": "within", "operatorValue": 5}]}], "parentId": "kjs19hje_12647204403674883", "conditionValue": "generic_funnel"}, {"type": "and", "mainData": [{"id": "kjs19qbj_17091245710758463", "nodeType": "eventType", "nodeTypeValue": "1mx6FkMoG0R8k9zgC5qDxfVagmW", "operatorValue": "true"}], "parentId": "kjs19hje_12647204403674883", "conditionValue": "segment"}], "conditionValue": "booleanCondition"}]', 1,'2021-01-12 03:39:53.310088', '2021-01-12 03:39:53.310088');
INSERT INTO segments (tenant_id, id, name, description, system, type, status, conditions, mark_as_deleted, created_at, updated_at) VALUES (1, '1mx7gfJWaB18ml66DRn514TtjJw', '12_01_2021_checkout_1_time_p02', 'segment single', 0, 'historical','published', '[{"type": "and", "isParent": true, "parentId": "root", "childrenAsList": [{"type": "and", "mainData": [{"id": "kjo4bxe0_46884200069683235", "step": [{"id": "kjo4bxe0_46884200069683235", "nodeType": "eventType", "operator": "greaterThan", "nodeTypeValue": "checkout_completed_(r)", "operatorValue": "1"}, {"id": "kjo4bxe0_2810139865001391", "nodeType": "windowType", "operator": null, "nodeTypeValue": "within", "operatorValue": 5}]}], "parentId": "kjo4bs83_10546329384505548", "conditionValue": "generic_funnel"}], "conditionValue": "booleanCondition"}]',1, '2021-01-12 03:51:40.750122', '2021-01-12 03:51:40.750122');
INSERT INTO segments (tenant_id, id, name, description, system, type, status, conditions, mark_as_deleted, created_at, updated_at) VALUES (1, '1mx7gl4FqORYAnqDcBae7nVyQhY', '12_01_2021_product_1_time_sn_p02', '12_01_2021_product_1_time_sn_p02', 0, 'live','published', '[{"type": "and", "isParent": true, "parentId": "root", "childrenAsList": [{"type": "and", "mainData": [{"id": "kjs19lq6_46931906582943594", "step": [{"id": "kjs19lq6_46931906582943594", "nodeType": "eventType", "operator": "greaterThan", "nodeTypeValue": "product_cart_added_(r)", "operatorValue": 1}, {"id": "kjs19lq6_785236355393115", "nodeType": "windowType", "operator": null, "nodeTypeValue": "within", "operatorValue": 5}]}], "parentId": "kjs19hje_12647204403674883", "conditionValue": "generic_funnel"}, {"type": "and", "mainData": [{"id": "kjs19qbj_17091245710758463", "nodeType": "eventType", "nodeTypeValue": "1mx7gfJWaB18ml66DRn514TtjJw", "operatorValue": "true"}], "parentId": "kjs19hje_12647204403674883", "conditionValue": "segment"}], "conditionValue": "booleanCondition"}]', 1,'2021-01-12 03:51:41.479794', '2021-01-12 03:51:41.479794');


INSERT INTO segments (tenant_id, id, name, description, system, type, status, conditions, mark_as_deleted, created_at, updated_at) VALUES (1, 's1', 'segment a', 'desc', 0, 'live', 'published', '[{"test": 1}]', 0, now64(), now64());


{'tenant_id': 1, 'segment_id': 's1', 'name': 'segment a', 'description': 'desc', 'system': 0, 'segment_type': 'live', 'status': 'published', 'conditions': "[{'test': 1}])", 'mark_as_deleted': 0, 'created_at': datetime.datetime(2021, 1, 13, 11, 6, 36, 427392), 'updated_at': datetime.datetime(2021, 1, 13, 11, 6, 36, 427397)}
{'tenant_id': 1, 'id': 's1', 'name': 'segment a', 'description': 'desc', 'system': 0, 'type': 'live', 'status': 'published', 'conditions': '[{"test": 1}])', 'mark_as_deleted': 0, 'created_at': datetime.datetime(2021, 1, 13, 11, 11, 26, 498481), 'updated_at': datetime.datetime(2021, 1, 13, 11, 11, 26, 498486)}
;
[{'tenant_id': 1, 'id': 's1', 'name': 'segment a', 'description': 'desc', 'system': 0, 'type': 'live', 'status': 'published', 'conditions': "[{'test': 1}])", 'mark_as_deleted': 0, 'created_at': datetime.datetime(2021, 1, 13, 11, 16, 47, 111704), 'updated_at': datetime.datetime(2021, 1, 13, 11, 16, 47, 111708)}]
select * from segments;


select tenant_id,
       id as segment_id,
       conditions
from segments
where type = 'live'
    and status = 'published'
;

select * from segment_user_preview;
select * from segment_user;


show create table segments;
select * from segments_v;


select * from segments;
select * from segments_v;
show create table segments;

select * from segment_user;

show tables;

show create table segment_total_user_ts_mv;
show create table segment_total_user_ts;

select * from segment_size;
drop table segment_size;
drop table segment_total_user_ts_mv;
drop table segment_total_user_ts;

show tables ;


drop table segment_size;
drop table segment_total_user_ts;
drop table segment_total_user_ts_mv;


show create table segment_user;
show create table segment_user_historical_queue;
show create table segment_user_preview;

select * from events;

select * from profile_str;


select * from events;
show create table events;

select * from segments;

select * from segment_funnel_temp;
show create table segment_funnel_temp;

drop table segment_funnel_temp;
CREATE TABLE eventify_stag.segment_funnel_temp
(
    `tenant_id` UInt16,
    `anonymous_id` String,
    `funnel_id` String
)
ENGINE = MergeTree()
ORDER BY (tenant_id, funnel_id)
;


insert into profile_str (anonymous_id, tenant_id, str_key, str_val, at)
values ('eu1b1z', 10, 'id', 'eu1b1z', '2021-ty_status', 'Platinum', '2021-01-15 13:35:00'),
('eu1b1z', 10, 'phone_number', '877 262 1446', '2021-01-15 13:35:00');


insert into profile_str (anonymous_id, tenant_id, str_key, str_val, at) values
    [('eu1b1z', 1, 'id', 'eu1b1z', '2021-01-15 13:37:57'),
 ('eu1b1z', 1, 'first_name', 'Dom', '2021-01-15 13:37:57')],
  ('eu1b1z', 10, 'last_name', 'eu1b1z', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'email', 'Dom_eu1b1z@primedata.ai', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'gender', 'Female', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'dob', '18/08/1999', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'acquisition_type', 'direct', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'country', 'Viet Nam', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'city', 'Hồ Chí Minh', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'street', 'Quốc Lộ 1a', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'timezone', 'Asia/Ho_Chi_Minh', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'registration_start_date', '01/10/2018', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'profile_image', 'https://robohash.org/animietminus.jpg?size=100x100&set=set1', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'app_version', '2.3.5', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'device_platform', 'iOS', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'device_os_version', '7.93', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'device_make', 'Apple', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'device_model', 'iPhone 9', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'preferred_language', 'Vietnamese', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'loyalty_status', 'Platinum', '2021-01-15 13:37:57'), ('eu1b1z', 10, 'phone_number', '877 262 1446', '2021-01-15 13:37:57')]
;


select * from profile_str where tenant_id = 10;
alter table profile_num delete  where tenant_id = 10;


insert into events (id, tenant_id, anonymous_id,
        event_name, session_id, scope,
        identity.keys, identity.vals,
        str_properties.keys, str_properties.vals,
        num_properties.keys, num_properties.vals,
        arr_properties.keys, arr_properties.vals,
        tags.keys, tags.vals,
        at )
        values ('EVENT_TEST', 10, 'eu1b1z', 'product_cart_added_(r)', '', '', [], [], [], [], ['properties'], [{'cart_id': 'book12'}], [], [], [], [], '2021-01-15 13:53:42'),('EVENT_TEST', 10, 'eu1b1z', 'product_cart_added_(r)', '', '', [], [], [], [], ['properties'], [{'cart_id': 'book12'}], [], [], [], [], '2021-01-15 13:53:42')




INSERT INTO segments (tenant_id, id, name, description, type, status, conditions, mark_as_deleted, created_at, updated_at)
VALUES (10, 'SEGMENT_TEST', 'Dom_r9oz2k_product_cart_30_times', '"Dom_r9oz2k_product_cart_30_times"', 'historical', 'SEGMENT_STATUS_PUBLISHED',
  '[{"id": "kjv4vwgb_21654690190433423", "isParent": True, "conditionValue": "booleanCondition", "type": "and", "isCollapsed": True, "parentId": "root", "childrenAsList": [{"id": "kjv4vwgb_8004415807135445", "isParent": False, "conditionValue": "funnel", "type": None, "parentId": "kjv4vwgb_21654690190433423", "childrenAsList": [], "isCollapsed": False, "mainDataTrait": [], "mainData": [{"id": "kjwozlsp_6354768348395872", "step": [{"id": "kjwozlsp_7964936026906688", "nodeType": "eventType", "nodeTypeValue": "product_cart_added_(r)", "operator": "greaterThan", "operatorValue": 1}, {"id": "kjwozlsp_30825569090747207", "nodeType": "eventProperty", "nodeTypeValue": "cart_id", "operator": "equals", "operatorValue": "book12", "propType": "str"}, {"id": "kjwozlsp_22929963457496116", "nodeType": "windowType", "nodeTypeValue": "within", "operatorValue": 5}]}, {"id": "kjwozlsq_4961826208160647", "step": [{"id": "kjwozlsq_6825069282423335", "nodeType": "eventType", "nodeTypeValue": "cart_viewed_(r)", "operator": "greaterThan", "operatorValue": 1}, {"id": "kjwozlsq_24222014621395016", "nodeType": "eventProperty", "nodeTypeValue": "products", "operator": "equals", "operatorValue": "comic", "propType": "str"}, {"id": "kjwozlsq_0073948583413774305", "nodeType": "windowType", "nodeTypeValue": "within", "operatorValue": 5}]}]}]}]',
  0, '2021-01-15 14:11:10', '2021-01-15 14:11:10');




alter table profile_str delete  where tenant_id = 10;
alter table profile_num delete  where tenant_id = 10;
alter table events delete  where tenant_id = 10;
alter table segments delete  where tenant_id = 10;


select * from events where tenant_id >=10;


select toDateTime(now64());



    (select t0.tenant_id as tenant_id, t0.anonymous_id as anonymous_id
    from
    (select tenant_id, anonymous_id
    from profile_str_final_v
    where tenant_id = 32
    and str_key = 'acquisition_type'
    and str_val = 'facebook')
as t0

 inner join
(select tenant_id, anonymous_id from profile_v where anonymous_id in(
(select tenant_id, anonymous_id
    from events
    where tenant_id = 1 and event_name = 'au_cart_viewed'

    and at >= '2021-01-15 09:10:40'
    and at <= '2021-01-15 09:10:45'
    group by tenant_id, anonymous_id
    having count() > 1)
)
as t1

 on t0.tenant_id=t1.tenant_id and t0.anonymous_id=t1.anonymous_id)



    (select t0.tenant_id as tenant_id, t0.anonymous_id as anonymous_id
    from
    (select tenant_id, anonymous_id
    from profile_str_final_v
    where tenant_id = 32
    and str_key = 'acquisition_type'
    and str_val = 'facebook')
as t0

 inner join
(select tenant_id, anonymous_id from profile_v where anonymous_id in

(select tenant_id, anonymous_id, count() as no_events
    from events
    where tenant_id = 1 and event_name = 'au_cart_viewed'

    and at >= '2021-01-15 09:10:40.423725'
    and at <= '2021-01-15 09:10:45.423725'
    group by tenant_id, anonymous_id
    having no_events > 1)
)
)
as t1

 on t0.tenant_id=t1.tenant_id and t0.anonymous_id=t1.anonymous_id);


select * from profile_str where tenant_id = 32;



( select t0.tenant_id as tenant_id, t0.anonymous_id as anonymous_id
from
    (select tenant_id, anonymous_id
    from profile_str_final_v
    where tenant_id = 32
    and str_key = 'acquisition_type'
    and str_val = 'facebook')
as t0

 inner join
( select tenant_id, anonymous_id from profile_v where anonymous_id in
( select anonymous_id from
( select tenant_id, anonymous_id, count() as no_events
from events
where tenant_id = 1 and event_name = 'au_cart_viewed'

and at >= '2021-01-15 11:14:42'
and at <= '2021-01-15 11:14:47'
group by tenant_id, anonymous_id
having no_events > 1 ) ) )
as t1
 on t0.tenant_id=t1.tenant_id and t0.anonymous_id=t1.anonymous_id )