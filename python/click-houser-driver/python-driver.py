from clickhouse_driver import Client

client = Client(
                host = 'localhost', 
                port = 9000, 
                database = 'primedata_test',
                )

client.execute('show tables')

# simple select
result = client.execute('select * from counter limit 3')

# parameterized to avoid sql injection 
from datetime import date
client.execute(
    'select  %(date)s, %(a)s + %(b)s',
    {'date': date.today(), 'a': 1,'b': 2}
)


# select data with process statistics
from datetime import datetime

progress = client.execute_with_progress(
    'select * from counter'
)

timeout = 20
started_at = datetime.now()

for num_rows, total_rows in progress:
    if total_rows:
        done = float(num_rows)/total_rows
    else:
        done = total_rows
    now = datetime.now()
    elapsed = (now - started_at).total_seconds()
    if elapsed > timeout and done < 0.0000001:
        client.cancel()
        break
    else:
        rv = progress.get_result()
        print(rv)
# chua hieu lam

# INSERT 
client.execute(
    'INSERT INTO counter(when,device, value) VALUES',
    [[datetime.now(), 1,10],
    [datetime.strptime('2018-01-03 12:12:12','%Y-%m-%d %H:%M:%S'), 2, 12]
    ]
)


# INSERT from CSV
from csv import DictReader
def iter_csv(filename):
    converters = {
        'when': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'),
        'device': int,
        'value': float
    }
    with open(filename, 'r') as f:
        reader = DictReader(f)
        for line in reader:
            yield {k: (converters[k](v) if k in converters else v) for k, v in line.items()}

client.execute('INSERT INTO counter VALUES', iter_csv('../../other/data.csv'))

# create table for test
CREATE TABLE test(
    x Int32,
    y Array(Int32)
)ENGINE = MergeTree()
ORDER BY x

insert into test values (100,[1,2,3])


#External data for query processing

tables = [{
    'name' : 'ext',
    'structure': [('x','Int32'),('y','Array(Int32)')],
    'data': [
        {'x': 100, 'y': [2, 4, 6, 8]},
        {'x': 500, 'y': [1, 3, 5, 7]}
    ]
}]

client.execute('select sum(x) from ext', external_tables = tables)

#Specifying query id
from uuid import uuid4
query_id = str(uuid4())
print(query_id)

client.execute('select * from db_test.test', query_id = query_id)

# Retrieving results in columnar form
client.execute('SELECT * from test array join y', columnar=True)

# Data types checking on INSERT
client.execute(
    'insert into test values',
    [
        [100,[3,2,1]],
        (111,[1,2,3]),
        (1,[1,2,3,4])
    ],
    types_check = True
)


#Query execution statistics 
# client.last_query


# support type
# Type       Insert   | Select
# Array : list, tuple | list
# LowCardinality(T)
# Nested (sequence of arrays) 
CREATE TABLE test_nested
(
    id String,
    `col` Nested(
    name String,
    version UInt16)
)
ENGINE = Memory

client.execute(
        'INSERT INTO test_nested VALUES', 
        [
            ('001',['a', 'b', 'c'], [100, 200, 300]),
            ['002',['a', 'b', 'c'], [100, 200, 300]]
        ]
)



# Test select materialized view
result = client.execute(
            'SELECT device, \
                    count(count) as count, \
                    maxMerge(max_value_state) as max, \
                    minMerge(min_value_state) as min, \
                    avgMerge(avg_value_state) as avg\
            FROM counter_daily GROUP BY device ORDER BY device',
            with_column_types = True,
            columnar = True
        )

print(result[0])


try:
    result = client.execute('''CREATE TABLE test3 (x Int32) 
                ENGINE = Memory''')
    print('Table was created success !')
except Exception as e:
    print(e.message())

