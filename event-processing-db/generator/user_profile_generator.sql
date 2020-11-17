insert into user_profile
    with
    ['Nam', 'Vu', 'Long', 'Thien'] as first_names,
    ['Nguyen Van', 'Hoang', 'Le Huu'] as last_names,
    ['Male','Female'] as genders,
    ['Hochiminh','Hanoi','Danang', 'Vungtau','Cantho'] as locations,
    [['facebook', 'google'],[ 'onesignal'], ['google','mail' ||
                                                      'chimp'],['facebook']] as channels
    select
           concat('a', toString(number)) as anonymous_id,
           rand(1)%1+1 as tenant_id,
           ['user_id', 'email','email'] as `identity.keys`,
           [concat('user-',randomPrintableASCII(5)), concat(randomPrintableASCII(5),'@gmail.com'), 'chile@gmail.com'] as `identity.vals`,
           ['first_name','last_name','gender','location_city', 'location_country'] as `str_properties.keys`,
           [first_names[rand(6)%length(first_names) + 1],last_names[rand(7)%length(last_names) + 1],genders[rand(2)%length(genders) + 1], locations[rand(3)%length(locations)+1],'Viet Nam' ] as `str_properties.vals`,
           ['first_activity','last_activity', 'days_since_last_order', 'total_revenue', 'total_orders','avg_order_value'] as `num_properties.keys`,
           [toUnixTimestamp(yesterday()+number),toUnixTimestamp(now()+number), toUnixTimestamp('2016-01-01 00:00:00')+number*60, rand(5)%300, rand(5)%100, rand(5)%100] as `num_properties.vals`,
           ['channels','arr_test_key'],
           [channels[rand(10)%length(channels) + 1],['arr_test_val']],
           now()
    from system.numbers
    limit 10
;


truncate table user_profile;
truncate table user_profile_final;
select * from user_profile_final_v where anonymous_id = 'a1';
select * from user_profile_final_v where tenant_id = 1 and anonymous_id = 'a0';



select count(*) from user_profile_final_v;

truncate table user_profile;
truncate table user_profile_final;

select rand()%2 + 1

