create table users(
    user_id Int16,
    name String
)Engine = Log()

create table orders(
    order_id Int16,
    price Int16
)Engine = Log()

create table user_order(
    user_id Int16,
    order_id Int16
)Engine = Log()

insert into users values (1,'chi'),(2,'le'), (3, 'van')

insert into user_order values (1,1), (1,2), (2, 3)

insert into orders values (1, 12), (2, 24), (3, 36)




select *
from users left join user_order on users.user_id = user_order.user_id
where user_order.user_id is
