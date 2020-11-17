from clickhouse_driver import Client
from datetime import datetime

client = Client(
    host='localhost',
    port=9000,
    database='test',
)

result = client.execute("""select now64()""")
result_1 = client.execute("""select now()""")
t = result[0][0]
t1 = result_1[0][0]

print(t)
print(t1)

def datetime_to_int(date_time: datetime) -> int:
    if date_time is None:
        return 0
    if date_time.tzinfo is not None:
        date_time = date_time.replace(tzinfo=None)

    timestamp = (date_time - datetime(1970, 1, 1)).total_seconds()
    return int(timestamp)

t = datetime_to_int(t)
t1 = datetime_to_int(t1)

print(t)
print(t1)




