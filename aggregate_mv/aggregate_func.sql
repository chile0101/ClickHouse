create table t(
    id Int32,
    x Int32,
    y String,
    z Array(String),
    n Nested(
        keys String,
        vals Float32
    )
)Engine = MergeTree()
order by id

insert into t values
(1, , 'str', ['zzz'], ['key1'], [12]), -- no insert number -> 0

insert into t values
(1,'12', 'str', ['zzz'], ['key1'], [12]) -- str -> number

insert into t values
(1,12, 123, ['zzz'], ['key1'], [12]) -- number -> str

insert into t values
(1,12, , ['zzz'], ['key1'], [12])   -- no insert string -> error

insert into t values
(1,12, 'str', [], ['key1'], [12]) -- empty array -> ['']

insert into t values
(12,12, 'str', [''], ['key1'], [12])

insert into t values
(1,12, 'str', ['zzz'], [], []) --  (1,12, 'str', ['zzz'], [], [])


insert into t values
(15,12, '', ['zzz'], [''], [12])

-----------------------------
insert into t values
(1, , 'str', ['zzz'], ['key1'], [12]),
(1, 1, 'str', ['zzz'], ['key1'], [12]),
(1, 1, 'str', ['zzz'], ['key1'], [12]),
(1, 3, 'str', ['zzz'], ['key1'], [12])


select count(x),
        sum(x),
        avg(x) ,
        sum(x)/ count(x)
from t































