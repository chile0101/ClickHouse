select sumForEach([1,2,3]);



create table test_resample(
    name String,
    age Int16,
    wage Int16
)Engine = MergeTree()
Order by name;

insert into test_resample
values ('John', 16, 10),
       ('Alice', 30, 15),
       ('Mary', 35, 8),
       ('velyn', 48, 11.5),
       ('David', 62, 9.9),
       ('Brian', 60, 16)
;

-- Let’s get the names of the people whose age lies in the intervals of [30,60) and [60,75).
select groupArrayResample(30, 75, 30)(name, age)
from test_resample;
[['Alice','Mary','velyn'],['Brian','David']]


--  count the total number of people and their average wage in the specified age intervals.
select countResample(30, 300, 30)(name, age) AS amount,
       avgResample(30, 300, 30)(wage, age) AS avg_wage
from test_resample;

[11.333333333333334,12.5]
--

insert into test_resample
values ('John', 101, 10),
       ('Alice', 201, 15)
;

select * from test_resample;

select countResample(0, 300, 30)(name, age) as amount,

from test_resample;



-------------If
select maxIf(age, name = 'John')
from test_resample;

