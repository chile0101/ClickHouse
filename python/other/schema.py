# from enum import Enum
#
#
# class Gender(Enum):
#     MALE = "Male"
#     FEMALE = "Female"
#
#     @staticmethod
#     def init_value():
#         d = {}
#         for i in Gender:
#             d.update({i.value: 0.0})
#         return d
#
# dic = Gender.init_value()
#
# print(dic)

# from datetime import datetime, timedelta
#
# def int_to_iso_timestamp(int_epoch: int):
#     utc_dt = datetime(1970, 1, 1) + timedelta(seconds=int_epoch)
#     print(type(utc_dt))
#     return utc_dt
#
# print(int_to_iso_timestamp(1600931072))
#
# print(datetime(2020, 9, 24, 6, 29, 28))

s = ["123","234","345"]

# kafka_brokers_formated = kafka_brokers.map(x -> f"kafka://%s".%s)

p = list(map(lambda x:  f"kafka://{x}", s))

print(p)
