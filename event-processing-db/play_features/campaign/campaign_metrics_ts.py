import pandas as pd
from clickhouse_driver import Client
from clickhouse_sqlalchemy import make_session, get_declarative_base
from sqlalchemy import create_engine, MetaData
from enum import  Enum
from functools import reduce

uri = 'clickhouse+native://default:primedata@10.110.1.5/eventify'

engine = create_engine(uri)
session = make_session(engine)
metadata = MetaData(bind=engine)
client = Client.from_url(uri)

Base = get_declarative_base(metadata=metadata)


def proxy_to_list_of_dict(result_proxy):
    dic, lst = {}, []
    for row_proxy in result_proxy:
        # row_proxy.items() returns an array like [(key0, value0), (key1, value1)]
        for column, value in row_proxy.items():
            # build up the dictionary
            dic = {**dic, **{column: value}}
        lst.append(dic)
    return lst


GET_CAMPAIGN_DATA = """
select *
from events_campaigns
    WHERE tenant_id = 1
        AND utm_campaign = 'campaign1'
        AND at >= FROM_UNIXTIME(1577836800)
        AND at <= FROM_UNIXTIME(1578009655)
"""

BASE_COLS = ['event_name', 'utm_campaign', 'utm_content', 'utm_source', 'gender']


def get_campaign_data(tenant_id, campaign_id, start, end):
    result = session.execute(GET_CAMPAIGN_DATA, {"tenant_id": tenant_id,
                                                 "campaign_id": campaign_id,
                                                 "start_time": start,
                                                 "end_time": end})
    data = proxy_to_list_of_dict(result)
    data_df = pd.DataFrame.from_dict(data)
    return data_df


def _campaign_data_base(df, base_columns):
    base_df = df[base_columns]
    base_df = base_df.drop_duplicates()
    print('BASE_DF', base_df)
    return base_df


def _net_revenue(df):
    new_columns = BASE_COLS + ['total_value']
    new_df = df[new_columns]
    revenue_df = new_df.groupby(BASE_COLS).agg({'total_value': 'sum'}).reset_index()
    revenue_df.rename(columns={"total_value": "revenue"}, inplace=True)
    revenue_df = revenue_df.astype({'revenue': 'float64'})
    print('REVENUE_DF', revenue_df)
    return revenue_df


def _num_of_customers(df):
    new_cols = BASE_COLS + ['anonymous_id']
    new_df = df[new_cols]

    customers_df = new_df.groupby(BASE_COLS).agg({'anonymous_id': 'count'}).reset_index()
    customers_df.rename(columns={'anonymous_id': 'num_of_customers'}, inplace=True)
    customers_df = customers_df.astype({'num_of_customers': 'float64'})
    print("CUSTOMERS_DF", customers_df)
    return customers_df


def _total_orders(df):
    new_df = df[BASE_COLS]
    orders_df = new_df.value_counts().reset_index()
    orders_df.rename(columns={0: "num_of_orders"}, inplace=True)
    print('ORDERS_DF', orders_df)
    return orders_df


def _avg_revenue_per_user(df):
    # Revenue
    revenue_df = _net_revenue(df)
    # print(revenue_df.dtypes)

    # Num of customers
    customers_df = _num_of_customers(df)

    new_df = pd.merge(revenue_df, customers_df, on=BASE_COLS)

    new_df['avg_revenue_per_user'] = new_df['revenue'] / new_df['num_of_customers']
    new_df.drop(['revenue', 'num_of_customers'], axis=1, inplace=True)
    print("REVENUE_PER_USER", new_df)
    return new_df


def _avg_value_per_order(df):
    # Revenue
    revenue_df = _net_revenue(df)
    # print(revenue_df.dtypes)
    # Num of customers
    orders_df = _total_orders(df)
    new_df = pd.merge(revenue_df, orders_df, on=BASE_COLS)

    new_df['avg_revenue_per_user'] = new_df['revenue'] / new_df['total_orders']
    new_df.drop(['revenue', 'total_orders'])
    print("REVENUE_PER_ORDER", new_df)
    return new_df


# def _get_metrics_transform():
#     return {
#         "kpi_revenue": _net_revenue,
#         # "kpi_net_revenue_frequent_buyer": _net_revenue_frequent_buyer,
#         "kpi_num_of_customers": _num_of_customers,
#         # "kpi_num_of_new_customers": _num_of_new_customers,
#         # "kpi_total_order_value": _total_order_value,
#         "kpi_avg_revenue_per_user": _avg_revenue_per_user,
#         "kpi_num_of_orders": _total_orders,
#         "kpi_avg_value_per_order": _avg_value_per_order
#     }


class CampaignKpiEnum(str, Enum):
    KPI_REVENUE = "kpi_revenue"
    KPI_NUM_OF_CUSTOMERS = "kpi_num_of_customers"
    KPI_NUM_OF_ORDERS = "kpi_num_of_orders"
    KPI_AVG_REVENUE_PER_USER = "kpi_avg_revenue_per_user"
    KPI_AVG_VALUE_PER_ORDER = "kpi_avg_value_per_order"


def _campaign_metrics_factory(metric):
    # TODO: Check metric in enum

    if metric == CampaignKpiEnum.KPI_REVENUE:
        return _net_revenue
    if metric == CampaignKpiEnum.KPI_NUM_OF_CUSTOMERS:
        return _num_of_customers
    if metric == CampaignKpiEnum.KPI_NUM_OF_ORDERS:
        return _total_orders
    if metric == CampaignKpiEnum.KPI_AVG_REVENUE_PER_USER:
        return _avg_revenue_per_user
    if metric == CampaignKpiEnum.KPI_AVG_VALUE_PER_ORDER:
        return _avg_value_per_order
    return None


df = get_campaign_data(tenant_id=1, campaign_id='campaign2', start=1577836800, end=1578009655)


metric_names = [CampaignKpiEnum.KPI_REVENUE,
                CampaignKpiEnum.KPI_NUM_OF_CUSTOMERS]

df_lst = []
for metric_name in metric_names:
    df_lst.append(_campaign_metrics_factory(metric_name)(df))

print(df_lst)
# df_merged = reduce(lambda left, right: pd.merge(left, right, on=BASE_COLS, how='outer'), df_lst)

# print(df_merged)






# base_df = _campaign_data_base(df, BASE_COLS)

# Revenue
# revenue_df = _net_revenue(df=df)
#
# # Num of orders
# orders_df = _total_orders(df)
#
# # Num of customers
# customers_df = _num_of_customers(df)

# avg_revenue_per_user_df = _avg_revenue_per_user(df)

# _avg_revenue_per_order = _avg_value_per_order(df)

# TODO Pass list error ?
# TODO: Try join with INDEX.
# TODO: Filter
