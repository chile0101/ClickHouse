# NET_REVENUE = """
# SELECT {base_cols},
#        SUM(total_value) AS {metric_name}
# FROM campaign_performance_data
# WHERE tenant_id = {tenant_id}
#     AND utm_campaign = '{utm_campaign}'
#     AND event_name_type = '{event_name_type}'
#     AND toUnixTimestamp(at) >= {start_time} AND toUnixTimestamp(at) <= {end_time}
# GROUP BY {base_cols}
# ORDER {base_cols}
# """
#
# CHANNEL_UNIQUE_VISITORS = """
# SELECT {base_cols},
#       COUNT(DISTINCT anonymous_id) AS {metric_name}
# FROM campaign_performance_data
# WHERE tenant_id = {tenant_id}
#    AND  utm_campaign = '{utm_campaign}'
#    AND event_name = '{event_name}'
#    AND toUnixTimestamp(at) >= {start_time} AND toUnixTimestamp(at) <= {end_time}
# GROUP BY {base_cols}
# ORDER {base_cols}
# """
#
# GOAL_COMPLETED = """
# SELECT {base_cols},
#       COUNT(DISTINCT anonymous_id) AS {metric_name}
# FROM campaign_performance_data
# WHERE tenant_id = {tenant_id}
#     AND  utm_campaign = '{utm_campaign}'
#     AND event_name_type = '{event_name_type}'
#     AND reach_goal_id = '{reach_goal_id}'
#     AND toUnixTimestamp(at) >= {start_time} AND toUnixTimestamp(at) <= {end_time}
# GROUP BY {base_cols}
# ORDER {base_cols}
# """
#
#
# def campaign_query_builder(query: str, base_cols, metric_name, tenant_id, utm_campaign, event_name='',
#                            event_name_type='', reach_goal_id='', *args, start_time, end_time):
#     return query.format(
#         base_cols=','.join(base_cols),
#         metric_name=metric_name,
#         tenant_id=tenant_id,
#         utm_campaign=utm_campaign,
#         event_name=event_name,
#         event_name_type=event_name_type,
#         reach_goal_id=reach_goal_id,
#         start_time=start_time,
#         end_time=end_time
#     )
#
#
# def get_campaign_data():
#     base_cols = ['utm_content', 'utm_source', 'gender', 'location']
#     tenant_id = 1
#     utm_campaign = 'CAMPAIGN Facebook'
#     event_name = 'click'
#     event_name_type = 'conversion'
#     start_time = 1606064400
#     end_time = 1606582799
#
#     net_revenue = campaign_query_builder(query=NET_REVENUE,
#                                          base_cols=base_cols,
#                                          metric_name='kpi_net_revenue',
#                                          tenant_id=tenant_id,
#                                          utm_campaign=utm_campaign,
#                                          event_name_type=event_name_type,
#                                          start_time=start_time,
#                                          end_time=end_time)
#     print(net_revenue)
#
#     channel_clicks = campaign_query_builder(query=CHANNEL_UNIQUE_VISITORS,
#                                             base_cols=base_cols,
#                                             metric_name='channel_clicks',
#                                             tenant_id=tenant_id,
#                                             utm_campaign=utm_campaign,
#                                             event_name=event_name,
#                                             event_name_type=event_name_type,
#                                             start_time=start_time,
#                                             end_time=end_time)
#     print(channel_clicks)
#
#     goal_competed = campaign_query_builder(query=GOAL_COMPLETED,
#                                            base_cols=base_cols,
#                                            metric_name='goal_completion',
#                                            tenant_id=tenant_id,
#                                            utm_campaign=utm_campaign,
#                                            event_name=event_name,
#                                            event_name_type=event_name_type,
#                                            reach_goal_id='reach_goal_id',
#                                            start_time=start_time,
#                                            end_time=end_time)
#     print(goal_competed)
#
#
#     metrics = []
#
# get_campaign_data()


from datetime import timedelta, datetime
from typing import List, Dict


def time_range(first_time, second_time):
    if first_time >= second_time:
        return []
    diff = second_time - first_time

    return [
        first_time + x for x in range(0, diff, 60*60)
    ]

def fill_missing_values(data: List[Dict], start_time: int, end_time: int, default_value=None):

    full_time_range = time_range(start_time, end_time)

    time_stamps = []
    for d in data:
        time_stamps.append(d['time_stamp'])

    missing_days = set(full_time_range) - set(time_stamps)

    for missing_time in missing_days:
        data.append(
            {
                'time_stamp': missing_time,
                'value': default_value
            }
        )

    data_sorted = sorted(data, key=lambda i: i['time_stamp'])

    return data_sorted


lst = [
    {
        "time_stamp": 1606294800,
        "value": 18
    },
    {
        "time_stamp": 1606298400,
        "value": 15
    }
]

data = fill_missing_values(data=lst, start_time=1606294800,
                           end_time=1606384800,
                           default_value=1)

print(data)
