    with [['m','f'] as genders, [['HCM','HN','DN', 'VT','CT'] as locations]]
    select
           rand(1)%10+1 as tenant_id,
           'u' as user_id,
           concat('a', toString(number)) as anonymous_id,
           ['gender','location'] as `str_properties.keys`,
           [genders[rand(2)%length(genders)+1], locations[rand(3)%length(locations)+1]] as `str_properties.vals`,
           ['total_values', 'last_order_at'] as `num_properties.keys`,
           [rand(4)%300, toUnixTimestamp('2016-01-01 00:00:00')+number*60] as `num_properties.vals`,
           [],[],
           now()
    from system.numbers
    limit 5


select number
from system.numbers
limit 5,6