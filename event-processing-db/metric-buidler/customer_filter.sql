( select t0.tenant_id as tenant_id, t0.anonymous_id as anonymous_id
from
    (select tenant_id, anonymous_id
    from profile_num_final_v
    where tenant_id = 1
    and num_key = 'avg_order_value'
    and num_val > 1)
as t0

 inner join
    (select tenant_id, anonymous_id
    from profile_num_final_v
    where tenant_id = 1
    and num_key = 'age'
    and num_val > 1)
as t1

 on t0.tenant_id=t1.tenant_id and t0.anonymous_id=t1.anonymous_id )

