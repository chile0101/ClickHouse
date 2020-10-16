create table test_float(
    id String,
    value32 Float32,
    value64 Float64
)Engine = MergeTree()
ORDER By id
;

┌─toUnixTimestamp(now())─┐
│             1601007051 │
└────────────────────────┘


insert into test_float values
('1', 1601007051, 1601007051)
;

┌─id─┬────value32─┬────value64─┐
│ 1  │ 1601007100 │ 1601007051 │
└────┴────────────┴────────────┘


select * from test_float;
┌─id─┬────value32─┬────value64─┐
│ 1  │ 1601007100 │ 1601007051 │
└────┴────────────┴────────────┘


insert into test_float values
('1', 1601008999, 1601008999)
;
┌─id─┬────value32─┬────value64─┐
│ 1  │ 1601009000 │ 1601008999 │
└────┴────────────┴────────────┘


insert into test_float values
('1', 2.999, 2.999)
;

