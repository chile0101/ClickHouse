1.Question 1 - 2
Consider the following collection of relation schemes:
professor(profname, deptname)
department(deptname, building)
committee(commname, profname)
1.1.Question 1:
Find all the professors who are in any one of the committees that professor Piper is in.
1.2.Question 2
Find all the professors who are in at least all those committees that professor Piper is in.
2.Question 3 - 5
Consider the following insurance database, where the primary keys are underlined
person(sin, name, address)
car(license, year, model)
owns(sin, license)
accident(ID, acc#, license, sin, date, city, damage)
2.1.Question 3
Find the total number of people whose cars were involved in accidents in 2002
2.2.Question 4
For each person involved in an accident, count the the number of accidents in which the person was driving a car belonging to them
2.3.Question 5
Delete the car 'Mazda' belonging to 'John Smith'.



select distinct profname
from committee as a
where not exists(
		select *
        from
		(
			select commname
            from committee
            where profname = 'Piper'
        ) as d
        left join
        (
			select commname
            from committee as b
            where a.profname = b.profname
        ) as e
        on d.commname = e.commname
        where e.commname is null
        )
;

-- Repeat
show tables;
professor(profname, deptname)
department(deptname, building)
committee(commname, profname)

create table professor(
    profname String,
    deptname String
)ENGINE = Log()
;

create table department(
    deptname String,
    building String
)ENGINE = Log()
;

create table committee(
    commname String,
    profname String
)ENGINE  = Log()
;

insert into committee values
('c1', 'chi'),
('c2', 'chi'),
('c1', 'huynh'),
('c2', 'huynh'),
('c1','chinh'),
('c3', 'chinh')
;



--     select commname
--     from
--     (
--         select commname
--         from committee
--         where profname = 'chi'
--     ) as c1 left join
--     (
--         select commname
--         from committee
--         where profname in (select distinct profname from committee where profname != 'chi')
--     ) as c2 on c1.commname = c2.commname
--     where c2.commname == ''
--
-- ;


-- --
-- select groupArray(commname) as arr
-- from committee
-- where profname = 'huynh'
-- having hasAll(arr,
--         (select groupArray(commname) as chi_arr
--         from committee
--         where profname = 'chi')
--     )
-- ;
-- with
-- (   select groupArray(commname)
--     from committee
--     where profname = 'chi'
-- ) as chi_arr
-- select *
-- from committee as c
-- having hasAll(
--     (select groupArray(commname)),
--     chi_arr
-- )
-- ;
--
--
-- with
-- (   select groupArray(commname)
--     from committee
--     where profname = 'chi'
-- ) as chi_arr
-- select
--        distinct outer.profname,
--        (
--            select groupArray(commname)
--            from committee as inner
--            where inner.profname = profname)
-- from committee as outer
-- where commname in ['c1','c2'];
--
--
-- (
--     select distinct profname
--     from committee
--     where profname != 'chi'
-- ) as c1 join
-- (
--     select groupArray(commname)
--     from committee
-- ) as c2 on c1.profname = c2.profname
-- ;
--
--
-- select
--        profname,
--        groupArray(commname)
-- from committee
-- where profname = 'chinh'
-- group by profname
--
-- with
-- (
--     select groupArray(profname)
--     from (
--             select  distinct profname
--     from committee
--     where profname != 'chi'
--              )
-- ) as profname_arr
-- select
--     arrayMap(x -> x, profname_arr)
-- ;
--
-- select
--        distinct profname
-- from committee as c1
-- where profname =
--     (select
--            profname,
--            groupArray(commname)
--     from committee as c2
--     where c2.profname = 'chi'
--     group by profname
-- )
-- ;
--
--
-- with (
--     select arrayDistinct(groupArray(profname))
--     from committee
--     ) as c1,
--   arrayEnumerate(length(c1)) as idx
-- select
--            profname,
--            groupArray(commname)
--     from committee as c2
--     where c2.profname =
--     group by profname
-- ;
--
-- with
--     [1,2,3] as arr
-- select arr[1]
--
-- with (
--     select groupArray(profname)
--     from
--        (
--            select profname
--            from committee
--        )
-- ) as name_arr,
-- (
--   select arrayZip(key, val)
--   from (
--         select groupArray(profname) as key,
--                groupArray(commname) as val
--         from committee
--            )
-- ) as key_val
-- select arrayFilter((x,y) -> x.1 == y, key_val, name_arr)
--
--
-- select
--     sumMap(arr)
-- from (
--       select sumMap(key, val)
--       from (
--             select groupArray(profname) as key,
--                    groupArray(commname) as val
--             from committee
--                )
--          )
-- ;
--
-- show tables;



with
(
    select arrayDistinct(groupArray(commname))
    from committee
    where profname = 'chi'
) as chi_arr
select
       profname,
       groupArray(commname) as other_arr
from committee
group by profname
having hasAll(other_arr, chi_arr)
;


