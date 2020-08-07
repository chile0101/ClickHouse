create table t(
    id UInt32,
    data String
)Engine = Log()

insert into t values
(1, 'chi'),(2,'le'),(3,'van')

insert into t
select
    number,
    concat('data', toString(number))
from system.numbers
limit 1000000

create view v as
select
    id,
    data
from t


select * from v
--0.076