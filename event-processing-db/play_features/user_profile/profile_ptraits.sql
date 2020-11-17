select;



show create table user_profile_final_v;


    anonymous_id,
    tenant_id,

    argMaxMerge(str_properties.keys) AS str_pros_keys,
    argMaxMerge(str_properties.vals) AS str_pros_vals,
    argMaxMerge(num_properties.keys) AS num_pros_keys,
    argMaxMerge(num_properties.vals) AS num_pros_vals,
    argMaxMerge(arr_properties.keys) AS arr_pros_keys,
    argMaxMerge(arr_properties.vals) AS arr_pros_vals,
    max(at_final) AS at
FROM eventify.user_profile_final
GROUP BY
    tenant_id,
    anonymous_id;



    SELECT
        tenant_id,
        anonymous_id,
        arrayZip(identity_keys, identity_vals) as identities,
        arrayFilter(x -> ((x.1) IN %(str_pros_filter)s), arrayZip(str_pros_keys, str_pros_vals)) AS str_pros,
        arrayFilter(x -> ((x.1) IN %(num_pros_filter)s), arrayZip(num_pros_keys, num_pros_vals)) AS num_pros,
        arrayFilter(x -> ((x.1) IN %(arr_pros_filter)s), arrayZip(arr_pros_keys, arr_pros_vals)) AS arr_pros,
        at
    FROM user_profile_final_v
    WHERE tenant_id = %(tenant_id)s AND anonymous_id = %(anonymous_id)s;

select * from user_profile where anonymous_id = 'DDF139AWtnvBcdrfMWzkd2oMFXj'
;


select * from segment_users where segment_id = '1jrhfosUoH2PK9OlA7MGFUeHgWn';






SELECT time_stamps[idx] AS time_stamp,
       total_users[idx] AS value
FROM
(
    SELECT
        groupArray(time_stamp) AS time_stamps,
        arrayFill(x -> (x != 0), groupArray(total_user)) AS total_users
    FROM
    (
        SELECT
            toStartOfFifteenMinutes(at) AS time_stamp,
            round(avg(length(users))) AS total_user
        FROM
        (
            SELECT DISTINCT *
            FROM segment_users
            WHERE (tenant_id = 1)
                AND (segment_id = '1jrhfosUoH2PK9OlA7MGFUeHgWn')
                AND (at <= '2020-11-05 09:36:00')
        )
        GROUP BY
            tenant_id,
            segment_id,
            time_stamp
        ORDER BY time_stamp WITH FILL FROM toDateTime('2020-11-03 00:00:00') TO toDateTime('2020-11-05 00:00:00') STEP 900
    )
)
ARRAY JOIN arrayEnumerate(time_stamps) as idx
WHERE time_stamp >= '2020-11-03 00:00:00'
  AND time_stamp <= '2020-11-05 00:00:00'
;


select * from user_profile where anonymous_id = 'DDF6xNiJd0SrZhFDTuzu9B8hHIy' order by  at desc ;


