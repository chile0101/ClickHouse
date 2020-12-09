SELECT tenant_id,
       anonymous_id,
       groupArray((str_key, str_val)) AS pros,
       max(toUnixTimestamp64Milli(at)) AS at
FROM profile_str_final_v
WHERE tenant_id = 0
  AND anonymous_id = 'DD9Cm3xVlLFVa9CVawn0AGsQt4v'
  AND str_key IN ['dob','firstVisit','first_name','gender','id']
GROUP BY tenant_id, anonymous_id;





select tenant_id,
       anonymous_id,
       str_key,
       str_val,
       at
from profile_str_final_v
WHERE tenant_id = 0
  AND anonymous_id = 'DD9Cm3xVlLFVa9CVawn0AGsQt4v'
  AND str_key IN ['dob','firstVisit','first_name','gender','id']
;
select * from profile_str_final_v where tenant_id = 0 and anonymous_id = 'DD9Cm3xVlLFVa9CVawn0AGsQt4v';


insert into profile_str values
('DD9Cm3xVlLFVa9CVawn0AGsQt4v',0,'dob','04/03/1988','2020-11-16 08:14:60.000');



SELECT tenant_id,
       anonymous_id,
       groupArray((num_key, num_val)) AS pros,
       max(toUnixTimestamp64Milli(at)) AS at
FROM profile_num_final_v
WHERE tenant_id = 1
  AND anonymous_id = 'DD9Cm3xVlLFVa9CVawn0AGsQt4v'
  AND num_key IN []
GROUP BY tenant_id, anonymous_id
;


select * from events;
