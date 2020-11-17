Form this result:

┌─f1──┬f2───────┬f3─────────────┐
│ 'a' │ [1,2,3] │ ['x','y','z'] │
│ 'b' │ [4,5,6] │ ['x','y','z'] │
└─────┴─────────┴───────────────┘
to :

┌─f1──┬x──┬y──┬z──┐
│ 'a' │ 1 │ 2 │ 3 │
│ 'b' │ 4 │ 5 │ 6 │
└─────┴───┴───┴───┘



select anonymous_id,
       `str_properties.keys`,
       `str_properties.vals`
from events_campaign;

select arrayEnumerate(`str_properties.keys`) from events_campaign;

select anonymous_id,
       `str_properties.keys`[num] as str_keys,
       `str_properties.vals`[num] as str_vals
from events_campaign
array join arrayEnumerate(`str_properties.keys`) as num;

select anonymous_id,
       `str_properties.keys`,
       `str_properties.vals`
from events_campaign array join str_properties


    KPI_NET_REVENUE = "kpi_net_revenue"
    KPI_NUM_OF_CUSTOMERS = "kpi_num_of_customers"
    KPI_NUM_OF_ORDERS = "kpi_num_of_orders"
    KPI_AVG_REVENUE_PER_USER = "kpi_avg_revenue_per_user"
    KPI_AVG_VALUE_PER_ORDER = "kpi_avg_value_per_order"


Time - Revenue:
1    2
2    5
4    9
5    10