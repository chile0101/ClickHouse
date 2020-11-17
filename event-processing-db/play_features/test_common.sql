select * from profile_str;

select * from profile_str_final_v;


select * from profile_num;

select * from profile_num_final_v;

select * from profile_arr;




    SELECT
        tenant_id,
        anonymous_id,
        arrayZip(identity_keys, identity_vals) as identities,
        arrayZip(str_pros_keys, str_pros_vals) AS str_pros,
        at
    FROM user_profile_final_v
    WHERE tenant_id = :tenant_id AND anonymous_id = :anonymous_id;


select tenant_id,
       anonymous_id,
       groupArray((num_key, num_val)) as pros,
       max(toUnixTimestamp64Milli(at))                        as at
from profile_num_final_v
where tenant_id = 2
  and anonymous_id = 'DDz2ygovDHBsZL2x1Mr6keJNTiE'
  and num_key IN ['age', 'lat', 'long', 'total_order', 'revenue']
group by tenant_id, anonymous_id;


select now64(); --2020-11-17 10:52:22.786
select toUnixTimestamp64Milli(now64());


select * from profile_num where anonymous_id = 'a7';
select * from profile_num_final_v where anonymous_id = 'a7';

select * from profile_str where anonymous_id = 'DDz2ygovDHBsZL2x1Mr6keJNTiE';
select * from profile_str_final_v where anonymous_id = 'DDz2ygovDHBsZL2x1Mr6keJNTiE';