from clickhouse_driver import Client

client = Client(
                host = 'localhost',
                port = 9000,
                database = 'test',
                )


# result = client.execute("""select arrayZip(arr_pros_key, arr_pros_val)
#                             from property_v""")
#
# result1 = client.execute("""select arrayZip(`arr_properties.keys`, `arr_properties.vals`)
#                             from property""")
#
# print(result1)
#
#
# [([('key1', ['val1'])],), ([('key2', ['val1', 'val2'])],)]
# [([('key1', ['val1'])],), ([('key2', ['val1', 'val2'])],)]
#
# create view property_v as
# select
#     id,
#     `arr_properties.keys` as arr_pros_key,
#     `arr_properties.vals` as arr_pros_val
# from property

# CREATE TABLE property
# (
#     `id` String,
#     `arr_properties.keys` Array(String),
#     `arr_properties.vals` Array(Array(String))
# )
# ENGINE = MergeTree
# ORDER BY id
# #
#  INSERT INTO property VALUES
# (1, ['key1'],[['val1']]),
# (2, ['key2'],[['val1','val2']])
#
#  INSERT INTO property VALUES
# (3, ['key1'], ['val1'],[['val2']]),
# (4, ['key2'], ['val2'],[['val2']])
#
# select arrayZip(property_keys, property_vals)
# from property










#---------------------------TEST TIME
result = client.execute("""select now()""")

print(result)