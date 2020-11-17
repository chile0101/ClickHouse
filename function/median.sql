select * from test;

select max(value) as max,
       min(value) as min,
       median(value) as median,
       avg(value) as avg
from test;

truncate table test;

insert into test values
('a1', 0, toDate(now()), now());

insert into test values
('a1', 10, toDate(now()), now());

insert into test values
('a1', 0, toDate(now()), now());

insert into test values
('a1', 6, toDate(now()), now());

select date()