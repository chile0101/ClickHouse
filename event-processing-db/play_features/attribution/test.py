import pandas as pd
from json import load


class AttributionDimension:
    def __init__(self, dimension, visit, conversion_rate, conversion, revenue_per_conversion, revenue):
        self.dimension = dimension
        self.visit = visit
        self.conversion_rate = conversion_rate
        self.conversion = conversion
        self.revenue_per_conversion = revenue_per_conversion
        self.revenue = revenue


def lists_to_dict(keys, values):
    if keys is None or values is None:
        return {}
    return dict(zip(keys, values))


def extract_value_by_key_parallel_array(key, keys_array, value_array):
    _dict = lists_to_dict(keys_array, value_array)
    return _dict.get(key, None)


df = pd.read_csv('data.csv', converters={'dimension.keys': eval, 'dimension.names': eval})

df_filtered = df[['dimension.keys', 'dimension.names', 'value_key', 'value_value']]

df_flattened = df_filtered
df_flattened['channel'] = df_flattened.apply(
    lambda r: extract_value_by_key_parallel_array('channel', r["dimension.keys"], r["dimension.names"]), axis=1)

df_aggregated = df_flattened[['channel', 'value_key', 'value_value']]
df_aggregated = df_aggregated.groupby(['channel', 'value_key']).agg({'value_value': 'sum'}).reset_index()

df_pivoted = df_aggregated.pivot(index='channel', columns='value_key', values='value_value').reset_index()

# full fill data
data = df_pivoted
data['visit'] = 100

# rate_calculation
data['conversion_rate'] = data['conversion'] / data['visit']
data['revenue_per_conversion'] = data['revenue'] / data['conversion']

# to_dict
channels = data.to_dict(orient='records')

channel_entities = []
for channel in channels:
    channel_entities.append(AttributionDimension(dimension=channel.get('channel'),
                                                 visit=channel.get('visit'),
                                                 conversion=channel.get('conversion'),
                                                 revenue=channel.get('revenue'),
                                                 conversion_rate=channel.get('conversion_rate'),
                                                 revenue_per_conversion=channel.get('revenue_per_conversion')))

print(channel_entities)
