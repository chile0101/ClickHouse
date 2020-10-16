create table order(
    user_id String,
    when DateTime,
    products Nested(
        keys String,
        vals Int32
    )
) Engine = MergeTree()
Partition by toYYYYMM(when)
Order by (user_id, when)


insert into order values
('001','2020-01-01 00:00:00', ['cafe','coca'], [12,24]),
('002','2020-01-01 00:01:00', ['cafe','coca'], [10,42]),
('003','2020-01-01 00:02:00', ['cafe','coca'], [4,8]),
('004','2020-01-01 00:03:00', ['cafe','coca'], [2,9]),
('001','2020-01-01 00:04:00', ['cafe','coca'], [12,24]),
('001','2020-02-01 00:01:00', ['cafe','coca'], [12,24]),
('002','2020-02-01 00:06:00', ['cafe','coca'], [12,24]),


-- Tinh tong so tien ma mot user mua cafe trong 1 ngay
-- C1 -- use array join
select
user_id,
toStartOfDay(when) as day,
sum(products.vals) as total_cafe_per_day
from order
array join products
where products.keys = 'cafe'
group by (user_id, day)
order by (user_id, day)

-- C2 -- use index
select
    user_id,
    toStartOfDay(when) as when,
    sum( products.vals[indexOf(products.keys, 'cafe')] ) as total_cafe_per_day
from order
group by (user_id, when)
order by (user_id, when)

-- user 4 mot ngay uong 2 cup cafe => sai
insert into order values ('004','2020-01-01 00:03:00', ['cafe','coca', 'cafe'], [2,9,10])

-- C3 Use arraySum

select
    user_id,
    toStartOfDay(when) as day,
    sum( arraySum(k,v -> if(k = 'cafe', v, 0),"products.keys","products.vals") ) as total_cafe_per_day
from order
group by (user_id, when)
order by (user_id, when)

----------------------------------filter ----------------------------------------

create table events(
    user_id String,
    when Date,
    type String,
    products Nested(
        phones String,
        time Int32
    )
) Engine = MergeTree()
Order by (user_id, when)

insert into events values
('001','2020-01-01','view', ['iphone7','iphoneX','oppo','xiaomi'], [6,7,3,7]),
('002','2020-01-01','view', ['iphone7','iphoneX','xiaomi','xiaomi'], [8,4,3,7]),
('003','2020-01-01','view',['iphone8','iphoneX','oppo','samsung'], [3,4,3,7]),
('004','2020-01-01','view',['iphone6','iphone8','oppo','xiaomi'], [6,4,3,7]),
('005','2020-01-01','view', ['iphone5','iphoneX','samsung','xiaomi'], [4,4,3,7]),

-- Filter nhung event view iphone trong khoang thoi gian >= 5 phut

select
    user_id,
    type,
    arrayFilter(
        (x,y ) -> x LIKE 'iphone%' and y >=5,
        products.phones,
        products.time
    ) as phone
from events

-- Lam sao ket hop them column time


select
    user_id,
    type,
    products.phones,
    products.time
from events
array join products
where
    products.phones LIKE 'iphone%'
    and products.time >= 5




------------------------------------ ARRAY MAP---------------------------------

-- Giả sử Iphone bị Bphone mua lại, giờ ta sửa tất cả đt iphone -> bph

alter table events update "products.phones" = arrayMap(p -> if(p LIKE 'iphone%', 'Bphone', p), "products.phones") where 1



----
create table arr(
    id UInt32,
    metrics_name Array(String),
    metrics_value Array(String)
) Engine = MergeTree()
Order by id

insert into arr values
(1,['n1','n2'],['v1','v2'])

insert into arr values
(2,['n1','n2','n3'],['v1','v2','v3'])


select id, metrics_name, metrics_value from arr

select id, name, value
from arr
array join metrics_name as name, metrics_value as value

--------------------------------- xem merge luu data the nao

create table par(
    id String,
    value Int16,
    type String,
    created_at DateTime
)Engine = MergeTree()
Partition by (toYYYYMM(created_at), type)
Order by (id, value)

insert into par values
('p1',1,'a' ,'2020-01-01 00:00:00'),
('p2',6,'a' , '2020-01-02 00:00:00'),
('p3',7, 'b' ,'2020-01-02 01:00:00'),
('p4',10,'b' , '2020-02-01 00:00:00'),
('p5',12, 'b' ,'2020-02-06 00:00:00')

insert into par values
('p6',14, '2021-01-01 00:00:00')



--------------------------------------Array Filter
create table users(
    id UInt16,
    identity Nested(
        keys String,
        vals String
    )
)ENGINE = MergeTree()
ORDER BY id
;

insert into users values
(1, ['name','email','email'],['chi','chi@gmail.com','le@gmail.com']);

insert into users values
(2, ['name','email','email'],['brian','brian@gmail.com','hung@gmail.com']);

select
       id,
       arrayFilter(
            (x, y) -> y LIKE 'email' and x != '' ,
            identity.vals,
            identity.keys
           )
from users;

