show tables;
drop table test;

create table test(
    utm_content String,
    utm_campaign String
) ENGINE = MergeTree()
ORDER BY utm_campaign;

insert into test values
('content 1', 'c1'),
('content 2', 'c1'),
('content 3', 'c2');

insert into test values
('content 2', 'c1'),
('content 3', 'c2');

select * from test;


select distinct utm_campaign from test;
select utm_content from test group by utm_campaign;


select * from test;

select utm_content,
       utm_campaign
from
(
    select arrayDistinct(groupArray(utm_content)) as distinct_arr,
           utm_campaign
    from test
    group by utm_campaign
)
array join distinct_arr as utm_content;