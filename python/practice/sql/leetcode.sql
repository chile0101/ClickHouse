-- FROM & JOINs determine & filter rows
-- WHERE more filters on the rows
-- GROUP BY combines those rows into groups
-- HAVING filters groups
-- SELECT
-- ORDER BY arranges the remaining rows/groups
-- LIMIT filters on the remaining rows/groups


-- Find Total Time Spent By Each Employee Problem
select event_day               as day,
       emp_id,
       sum(out_time - in_time) as total_time
from employees
group by day, emp_id
;


-- Invalid Tweets Problem
select tweet_id
from tweets
where length(content) > 15
;

-- Daily Leads And Partners Problem

select date_id,
       make_name,
       count(distinct lead_id)         as unique_leads,
       count(distinct unique_partners) as unique_partners
from DailySales
group by date_id, make_name
;

-- Students With Invalid Departments Problem

select id, name
from Students as s
where department_id not in (select id
                            from Departments)
;

-- 1378: Replace Employee ID With The Unique Identifier Problem

select e.name       as name,
       eu.unique_id as unique_id
from Employees as e
         left join EmployeeUNI as eu
                   on e.id == eu.id
;

-- 1571 Warehouse Manager Problem

select name                                   as warehouse_name,
       sum((width * length * height) * units) as volume
from Warehouse as w
         inner join Products as p
                    on w.product_id = p.product_id
group by warehouse_name
;

-- 1587 Bank Account Summary II Problem

select name, balance
from Users as u
         join
     (
         select account,
                sum(amount) as balance
         from Transactions
         group by account
         having balance > 10000
         ) as t
     on u.account == t.account
;

-- 1303 Find The Team Size Problem
select employee_id,
       team_size
from Employee as e
         join (
    select team_id,
           count(employee_id) as team_size
    from Employee
    group by team_id
    ) as t
              on e.team_id == t.team_id


-- 1581 Customer Who Visited But Did Not Make Any Transactions Problem

select customer_id,
       count(visit_id) as count_no_trans
from Visits
where visit_id not in (select distinct visit_id from Transactions)
group by customer_id

-- 1783 Grand Slam Titles Problem

select player_id, play_name, grand_slams_count
from Players as p
         join
     (
         select player_id, sum(count) as grand_slams_count
         from (
               select Wimbledon as player_id, count(Wimbledon) as count
               from Championships
               group by Wimbledon
               union all
               select Fr_open as player_id, count(Fr_open)
               from Championships as count
               group by Fr_open
               union all
               select US_open as player_id, count(US_open)
               from Championships as count
               group by US_open
               union all
               select Au_open as player_id, count(Au_open)
               from Championships as count
               group by Au_open
                  )
         group by player_id
         ) as g
     on p.player_id == g.player_id
;

-- 1393 Capital Gain/Loss

select stock_name,
       sum(if(operation = 'Sell', price, -price)) as capital_gain_loss
from Stocks
group by stock_name
;

-- 1445 Apples & Oranges Problem

select sale_date,
       sum(if(fruit = 'apples', sold_num, -sold_num)) as diff
from Sales
group by sale_date

-- 1270 All People Report To The Given Manager Problem

(select employee_id
from Employees
where manager_id = 1
) as d1
union all
(
select employee_id
from Employees
where manager_id in d1
) as d2
union all
(
select employee_id
from Employees
where manager_id in d2
) as d3

;


show tables;
select * from events where anonymous_id = '1qSWf6zv0OdtIrxPLvbNbAPM5S2';

select * from profile_num where anonymous_id = '1qSWf6zv0OdtIrxPLvbNbAPM5S2';
select * from profile_str where anonymous_id = '1qSWf6zv0OdtIrxPLvbNbAPM5S2';




select str_val as value
from profile_str_final_v
where tenant_id = 1 and str_key = 'email'

and anonymous_id in (
select
   anonymous_id
from
(
  select anonymous_id,
        groupArray(num_key) as keys,
        groupArray(num_val) as vals
  from profile_num_final_v
  where tenant_id = 1
        and num_key in ['age']
  group by tenant_id, anonymous_id
)
where lessOrEquals(vals[indexOf(keys, 'age')],1)
)

group by str_val
order by count(str_val) desc
limit 1


#262. Trips and Users

with reqs as (
    select Client_Id, Status, Banned, Role, Request_at
    from Trips t
             inner join Users u
                        on t.Client_Id = u.Users_Id
    where Banned = 'No' and Request_at between '2013-10-01' and '2013-10-03'
)
select Request_at as Day, round(count(If(Status like 'cancelled_by_%', 1, NULL))/ count(*), 2) as 'Cancellation Rate'
from reqs
group by Request_at




