
DROP TABLE IF EXISTS requests;
CREATE TABLE requests (
  request_date Date,
  request_time DateTime,
  response_time Int,
  request_uri String)
ENGINE  = MergeTree(request_date, (request_time, request_uri), 8192);
DROP TABLE IF EXISTS requests_graph;
CREATE MATERIALIZED VIEW requests_graph
ENGINE = AggregatingMergeTree(request_date, (request_hour,request_uri), 8192)
AS SELECT
  request_date,
  toStartOfHour(request_time) request_hour,
  request_uri,
  avgState(response_time) avg_response_time,
  maxState(response_time) max_response_time,
  minState(response_time) min_response_time,
  countState()            request_count
FROM requests
GROUP BY request_date, request_hour, request_uri;
insert into requests values (today(), now(), 10, 'ya.ru'),
                            (today(), now(), 20, 'ya.ru'),
                            (today(), now(), 10, 'google.ru'),
                            (today(), now()+7200, 20, 'google.ru');
SELECT request_date,
       request_hour,
       request_uri,
       avgMerge(avg_response_time) avg_response_time,
       maxMerge(max_response_time) max_response_time,
       minMerge(min_response_time) min_response_time,
       countMerge(request_count) request_cnt
FROM requests_graph
GROUP BY request_date, request_hour, request_uri;
┌─request_date─┬────────request_hour─┬─request_uri─┬─avg_response_time─┬─max_response_time─┬─min_response_time─┬─request_cnt─┐
│   2018-03-31 │ 2018-03-31 03:00:00 │ ya.ru       │                15 │                20 │                10 │           2 │
│   2018-03-31 │ 2018-03-31 05:00:00 │ google.ru   │                20 │                20 │                20 │           1 │
│   2018-03-31 │ 2018-03-31 03:00:00 │ google.ru   │                10 │                10 │                10 │           1 │
└──────────────┴─────────────────────┴─────────────┴───────────────────┴───────────────────┴───────────────────┴─────────────┘
SELECT count() FROM requests_graph;
┌─count()─┐
│       4 │
└─────────┘
OPTIMIZE TABLE requests_graph;
SELECT count() FROM requests_graph;
┌─count()─┐
│       3 │
└─────────┘
SELECT request_date,
       request_hour,
       request_uri,
       avgMerge(avg_response_time) avg_response_time,
       maxMerge(max_response_time) max_response_time,
       minMerge(min_response_time) min_response_time,
       countMerge(request_count) request_cnt
FROM requests_graph
GROUP BY request_date, request_hour, request_uri;
┌─request_date─┬────────request_hour─┬─request_uri─┬─avg_response_time─┬─max_response_time─┬─min_response_time─┬─request_cnt─┐
│   2018-03-31 │ 2018-03-31 03:00:00 │ ya.ru       │                15 │                20 │                10 │           2 │
│   2018-03-31 │ 2018-03-31 05:00:00 │ google.ru   │                20 │                20 │                20 │           1 │
│   2018-03-31 │ 2018-03-31 03:00:00 │ google.ru   │                10 │                10 │                10 │           1 │
└──────────────┴─────────────────────┴─────────────┴───────────────────┴───────────────────┴───────────────────┴─────────────┘
------------------------------------
# modern syntax and MV to
CREATE table requests_graph (
request_date Date,
request_hour DateTime('UTC'),
request_uri String,
avg_response_time AggregateFunction(avg, Int32),
max_response_time AggregateFunction(max, Int32),
min_response_time AggregateFunction(min, Int32),
request_count AggregateFunction(count)
) ENGINE = SummingMergeTree() Partition by toYYYYMM(request_date)
  Order by (request_hour,request_uri);


  CREATE MATERIALIZED VIEW requests_graph_mv
  to requests_graph
  AS SELECT
    request_date,
    toStartOfHour(request_time) request_hour,
    request_uri,
    avgState(response_time) avg_response_time,
    maxState(response_time) max_response_time,
    minState(response_time) min_response_time,
    countState()            request_count
  FROM requests
  GROUP BY request_date, request_hour, request_uri;