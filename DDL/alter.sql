  CREATE TABLE segment_agg(
    id String,
    data String,
    time_stamp Date,
    metric_name String,
    metrics_agg Nested(
        keys String,
        vals Float32
    )
)ENGINE = MergeTree()
PARTITION BY toYYYYMM(time_stamp)
ORDER BY(id, time_stamp, metric_name)

-- id -> segment_id
ALTER TABLE segment_agg RENAME COLUMN "data" TO "bigdata"
alter table segment_agg modify column "bigdata" Int16


-- Date -> DateTime -- chua tim dc solution
alter table segment_agg modify column time_stamp DateTime --DB::Exception: ALTER of key column time_stamp is forbidden.

-- delete time_stamp from primary key -- chua tim dc solution
alter table segment_agg modify order by (id, metric_name)