select * from user_profile_final_v where tenant_id = 1 and anonymous_id = 'a0';

select arrayFilter(key -> key.1 LIKE '%email%', arrayZip(identity_keys, identity_vals))
from user_profile_final_v
where tenant_id = 1 and anonymous_id = 'a0';




show create table user_profile_final_v;


show tables;

show create table events;
show create table user_profile_final_v;


