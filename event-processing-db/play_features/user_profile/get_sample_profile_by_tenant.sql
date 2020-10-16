SELECT
    anonymous_id,
    tenant_id,
    argMaxMerge(identity.keys) AS identity_keys,
    argMaxMerge(identity.vals) AS identity_vals,
    argMaxMerge(str_properties.keys) AS str_pros_keys,
    argMaxMerge(str_properties.vals) AS str_pros_vals,
    argMaxMerge(num_properties.keys) AS num_pros_keys,
    argMaxMerge(num_properties.vals) AS num_pros_vals,
    argMaxMerge(arr_properties.keys) AS arr_pros_keys,
    argMaxMerge(arr_properties.vals) AS arr_pros_vals,
    max(at_final) AS at
FROM primedata_test.user_profile_final


SELECT
tenant_id,
segment_id,
metric_name,
arrayZip(metrics_agg_keys, metrics_agg_vals) as metrics_agg
FROM segment_agg_final_v
WHERE (tenant_id = 1) AND (segment_id = 's1') AND (metric_name = 'source')

insert into user_profile
    with
    ['Nam', 'Vu', 'Long', 'Thien'] as first_names,
    ['Nguyen Van', 'Hoang', 'Le Huu'] as last_names,
    ['Male','Female'] as genders,
    ['Hochiminh','Hanoi','Danang', 'Vungtau','Cantho'] as locations,
    ['Google Ads','Google Search','Facebook Content', 'Others'] as sources
    select
           concat('a', toString(number)) as anonymous_id,
           rand(1)%1+1 as tenant_id,
           ['user_id', 'email'] as `identity.keys`,
           [concat('user-',randomPrintableASCII(5)), concat(randomPrintableASCII(5),'@gmail.com')] as `identity.vals`,
           ['first_name','last_name','gender','location_city', 'source','location_country'] as `str_properties.keys`,
           [first_names[rand(6)%length(first_names) + 1],last_names[rand(7)%length(last_names) + 1],genders[rand(2)%length(genders)+1], locations[rand(3)%length(locations)+1], sources[rand(4)%length(sources)+1], 'Viet Nam' ] as `str_properties.vals`,
           ['first_activity','last_activity','total_value', 'last_order_at'] as `num_properties.keys`,
           [toUnixTimestamp(yesterday()+number),toUnixTimestamp(now()+number),rand(5)%300, toUnixTimestamp('2016-01-01 00:00:00')+number*60] as `num_properties.vals`,
           [''],[[]],
           now()
    from system.numbers
    limit 1000

    limit 1000000
    limit 900000,100000

;





WITH
    (
        SELECT count(*)
        FROM user_profile_final_v
        WHERE tenant_id = 1
    ) AS total_rows
SELECT
    tenant_id,
    anonymous_id,
    arrayZip(identity_keys, identity_vals),
    arrayFilter(x -> ((x.1) IN ['first_name', 'last_name']), arrayZip(str_pros_keys, str_pros_vals)) AS str_pros,
    arrayFilter(x -> ((x.1) IN []), arrayZip(num_pros_keys, num_pros_vals)) AS num_pros,
    arrayFilter(x -> ((x.1) IN []), arrayZip(arr_pros_keys, arr_pros_vals)) AS arr_pros,
    at,
    total_rows
FROM user_profile_final_v
WHERE tenant_id = 1
LIMIT 0 * 10, 10;






