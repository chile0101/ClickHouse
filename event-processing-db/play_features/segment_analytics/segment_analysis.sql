



show tables;

select tenant_id,
       segment_id,
       metric_name,
       arrayZip(metrics_agg_keys, metrics_agg_vals)
from segment_agg_final_v
where tenant_id = 1 and segment_id = 's1' and metric_name = 'gender';






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
           ['first_name','last_name','gender','location_city', 'source'] as `str_properties.keys`,
           [first_names[rand(6)%length(first_names) + 1],last_names[rand(7)%length(last_names) + 1],genders[rand(2)%length(genders)+1], locations[rand(3)%length(locations)+1], sources[rand(4)%length(sources)+1] ] as `str_properties.vals`,
           ['first_activity','last_activity', 'days_since_last_order', 'total_revenue', 'total_orders','avg_order_value'] as `num_properties.keys`,
           [toUnixTimestamp(yesterday()+number),toUnixTimestamp(now()+number), toUnixTimestamp('2016-01-01 00:00:00')+number*60*60*24, rand(5)%300, rand(5)%100, rand(5)%100] as `num_properties.vals`,
           [''],[[]],
           now()
    from system.numbers
    limit 1000

    limit 1000000
    limit 900000,100000

;

insert into segment_users values
(1, '1hnoZOoU6KKfd2R5JHb0pnUv67H', ['a1','a2','a3','a4','a5','a6','a7','a8','a9','a10','a11','a12','a13','a14','a15','a16'], now()),
(1, 's2', ['a1','a2','a3','a4','a5','a6','a7'], now()),
 (1, 's3', ['a1','a2','a10','a12','a24'], now()),
  (1, 's4', ['a30','a46','a76'], now()),
;





SELECT
    ['max', 'avg', 'med', 'min'] AS metric_key,
    [toFloat64(maxOrDefault(days)), avgOrDefault(days), medianOrDefault(days), toFloat64(minOrDefault(days))] AS metric_value
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
            WHERE tenant_id =1 AND segment_id = 's1'
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
            num_vals[indexOf(num_keys,'last_order_at')] AS t
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
)
;




SELECT
    ['max', 'avg', 'med', 'min'] AS metric_key,
    [maxOrDefault(v), avgOrDefault(v), medianOrDefault(v), minOrDefault(v)] AS metric_value
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
        WHERE tenant_id = 1 AND segment_id = 's1'
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
        num_vals[indexOf(num_keys,'revenue')] AS v
    FROM
    (
        SELECT
            tenant_id,
            anonymous_id,
            argMaxMerge(num_properties.keys) AS num_keys,
            argMaxMerge(num_properties.vals) AS num_vals
        FROM user_profile_final
        WHERE tenant_id =1
        GROUP BY
                 tenant_id,
                 anonymous_id
    )
) AS pf ON (sf.users = pf.anonymous_id) AND (sf.tenant_id = pf.tenant_id)
WHERE v >= 0

;


select * from user_profile_final_v where tenant_id = 1 and anonymous_id = 'a1';


        SELECT
            tenant_id,
            anonymous_id,
            arrayZip(identity_keys, identity_vals) AS identities,
            arrayFilter(x -> ((x.1) IN []), arrayZip(str_pros_keys, str_pros_vals)) AS str_pros,
            arrayFilter(x -> ((x.1) IN ['revenue']), arrayZip(num_pros_keys, num_pros_vals)) AS num_pros,
            arrayFilter(x -> ((x.1) IN []), arrayZip(arr_pros_keys, arr_pros_vals)) AS arr_pros,
            at
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
                    users
                FROM segment_users_final_v
                WHERE (tenant_id =1) AND (segment_id ='s1')
            )
            ARRAY JOIN users
        ) AS s
        INNER JOIN user_profile_final_v AS p
            ON (s.tenant_id = p.tenant_id) AND (s.users = p.anonymous_id)
;


