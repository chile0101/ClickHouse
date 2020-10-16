create table station(
    id String,
    city String
)Engine = Log()

insert into station values
('1', 'hcm'),
('2','hn'),
('3','danang')
;

insert into station values
('4', 'a1'),
('5','e1'),
('6','fgh')
;

select city, len
from (
      select city, length(city) as len
      from station
      group by city

)
;


select city
from
      select city, length(city) as len
      from station
      group by city
     order by len, city
         )

select city, len
from (
      select city, length(city) as len
      from station
      group by city
      order by len desc , city
      limit 1
) as s1
union all (
      select city, length(city) as len
      from station
      group by city
     order by len asc , city
    limit 1
)
;

select distinct city
from (
      with ['a', 'e', 'i', 'o', 'u'] as vowels
      select city,
            substring(city, 1, 1) as first_c
      from station
      where first_c in vowels
         )
