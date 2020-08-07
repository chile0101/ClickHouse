-- Question 1
select city, count() as customers
from customer
group by city
where >= 2

-- Question 2
select lastname, firstname
from customer
where lastname LIKE 'W%'
order lastname, firstname


-- Question 3
select *
from customer
where
    (zipcode = '75000' or zipcode = '34000')
    and birthday != Null and birthday != ''



create table customers(
    id Int16,
    name String,
    city String
)Engine = Log();


insert into customers values
(1, 'chi', 'HCM'),
(2, 'huynh','HCM'),
(3,'wagt', 'HN'),
(4,'tho', 'DN'),
 (5, 'aaa', 'HCM'),
(6,'wabc', 'HN'),
(7,'chi', 'HCM')

select
        name
from customers
where name LIKE 'w%'
order by name


select
        distinct name,
        city
from customers
order by name;










select * from customers;

select * from
    (
        select city, count(id) as c
        from customers
        group by city
    )
where  c >=2


-- Customers
1,chi,HCM
2,huynh,HCM
3,long,HN
4,tho,DN
5,aaa,HCM
6,bbb,HN


--
HCM,3
DN,1
HN,2

