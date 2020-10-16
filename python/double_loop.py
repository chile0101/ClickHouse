# class Metric(object):
#     def __init__(self, key: str, value: float):
#         self.key = key
#         self.value = value
#
#
# def total_metric_value(metrics_agg ) -> float:
#     return sum(map(lambda metric: metric.value,metrics_agg))
#
#
# metrics = []
# # metrics.append(Metric(key='hcm',value=1))
# # metrics.append(Metric(key='hn',value= 2))
# # metrics.append(Metric(key='dn',value= 3))
#
#
# total = total_metric_value(metrics)
#
# print(total)


# identities = [('email', 'abc@gmail.com'), ('user_id','123'), ('email', 'chile@gmail.com')]
#
# emails = list(filter(lambda x: x[0] == 'email', identities))
#
# print(emails)

# identities_d = dict(identities)
#
# print(identities_d)
#
# string = "toi ten la {name}, toi {old} tuoi , {name}"
#
# new_str = string.format(name="chi")
#
# print(new_str)
from typing import List, Tuple
#
# def unzip_list_of_tuple(lst: List[Tuple]) -> List:
#     return list(zip(*lst))
#
# def extract_keys_vals(lst: List[Tuple]):
#     unzip_lst = unzip_list_of_tuple(lst)
#     return unzip_lst[0], unzip_lst[1]
#
# lst = [('name','chi'),('old',12)]

# result = extract_keys_vals(lst)
#
# keys, vals = extract_keys_vals(lst)
#
# print(keys)
#
# print(vals)

# d = dict(lst)
#
# print(list( d.keys()))
# print(d.values())

# dic0 = {'dic0': 0}
# dic1 = {'dic1': 1}
# dic3 = {'name':"chi", "old": 123}
#
# dic2 = dict(dic0, **dic1, **dic3)
#
#
# print(dic2)


# d = {'anonymous_id': 'a1', 'tenant_id': 25, 'identities': {'user_id': 1}, 'str_pros': {'gender': 'male'}, 'num_pros': {},
#  'arr_pros': {}}
#
# identities = d.pop("identities")
#
# print(d)
# print(identities)

from enum import Enum


class SegmentReportMetricNameEnum(str, Enum):
    TOTAL_CUSTOMER = "total_customer"

    GENDER = "gender"
    LOCATION_CITY = "location_city"
    SOURCE = "source"

    REVENUE = "total_revenue"
    TOTAL_ORDERS = "total_orders"
    AVG_ORDER_VALUE = "avg_order_value"

    DAYS_SINCE_LAST_ORDER = "days_since_last_order"


AVG_ORDER_VALUE = "avg_order_value"
REVENUE = "revenue"
CURRENCY = "currency"
LAST_ORDER_AT = "last_order_at"
TOTAL_ORDER = "total_order"
TOTAL_VALUE = "total_value"


class MetricNameKeyMapping(Enum):
    SegmentReportMetricNameEnum.REVENUE: REVENUE
    SegmentReportMetricNameEnum.AVG_ORDER_VALUE: LAST_ORDER_AT
