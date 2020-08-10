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


--------------------------------------------
INSERT INTO billy.readings (sensor_id, time, temperature)
WITH
  toDateTime(toDate('2019-01-01')) as start_time,
  1000000 as num_sensors,
  365 as num_days,
  24*60 as num_minutes,
  num_days * num_minutes as total_minutes
SELECT
  intDiv(number, num_minutes) % num_sensors as sensor_id,
  start_time + (intDiv(number, num_minutes*num_sensors) as day)*24*60*60 + (number % num_minutes as minute)*60 time,
  60 + 20*sin(cityHash64(sensor_id)) /* median deviation */
  + 15*sin(2*pi()/num_days*day) /* seasonal deviation */
  + 10*sin(2*pi()/num_minutes*minute)*(1 + rand(1)%100/2000) /* daily deviation */
  + if(sensor_id = 473869 and
       time between '2019-08-27 13:00:00' and '2019-08-27 13:05:00', -50 + rand(2)%100, 0)
       /* sensor outage, generates huge error on 2019-08-27 */
  as temperature
FROM numbers_mt(525600000000)
SETTINGS max_block_size=1048576;