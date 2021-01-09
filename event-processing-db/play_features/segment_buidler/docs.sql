drop table event;
create table event(
    user String,
    time UInt16,
    event_name String
)
ENGINE = Log();

-- identify , view, search, add to cart, checkout_started, checkout_completed.

insert into event values ('u1',1, 'identify');
insert into event values ('u1',2, 'view');
insert into event values ('u1',3, 'search');
insert into event values ('u1',5, 'add_to_cart');
insert into event values ('u1',10, 'checkout');


insert into event values ('u2',1, 'identify');
insert into event values ('u2',2, 'view');
insert into event values ('u2',4, 'view');


insert into event values ('u3',5, 'identify');
insert into event values ('u3',8, 'view');
insert into event values ('u3',12, 'search');


insert into event values ('u4',8, 'identify');
insert into event values ('u4',10, 'view');
insert into event values ('u4',12, 'search');
insert into event values ('u4',13, 'add_to_cart');




insert into event values ('u5',7, 'identify');
insert into event values ('u5',9, 'view');
insert into event values ('u5',12, 'search');
insert into event values ('u5',15, 'add_to_cart');
insert into event values ('u5',18, 'add_to_cart');


insert into event values ('u6',8, 'identify');
insert into event values ('u6',9, 'view');
insert into event values ('u6',12, 'search');
insert into event values ('u6',16, 'add_to_cart');
insert into event values ('u6',20, 'add_to_cart');
insert into event values ('u6',30, 'checkout');

insert into event values ('u1',32, 'identify');
insert into event values ('u1',34, 'view');

insert into event values ('u4',32, 'view');
insert into event values ('u4',40, 'add_to_cart');
insert into event values ('u4',42, 'checkout');

insert into event values ('u7',46, 'identify');

insert into event values ('u8',50, 'view');
insert into event values ('u8',52, 'view');

insert into event values ('u9',52, 'identify');
insert into event values ('u9',54, 'view');
insert into event values ('u9',56, 'view');
insert into event values ('u9',58, 'identify');
insert into event values ('u9',60, 'view');


insert into event values ('u10',54, 'identify');
insert into event values ('u10',56, 'view');
insert into event values ('u10',58, 'view');
insert into event values ('u10',59, 'add_to_cart');
insert into event values ('u10',60, 'checkout');


insert into event values ('u11',54, 'identify');
insert into event values ('u11',56, 'identify');
insert into event values ('u11',58, 'view');
insert into event values ('u11',59, 'add_to_cart');
insert into event values ('u11',60, 'checkout');

-------------

SELECT * FROM event where event_name = 'view' order by user, time;


----------
SELECT user,
       sequenceCount('(?1)(?1)(?t<=5)(?2)')(time, event_name='identify', event_name='view') AS count
FROM event
GROUP BY user
;



-----------------------

select * from (
SELECT user,
       sequenceCount('(?1).*(?1)(?t<=5)(?2)')(time, event_name='identify', event_name='view') AS count
FROM event
GROUP BY user
)
where count > 0;











-- view 10s and then checkout
SELECT * FROM (
    SELECT user,
           sequenceCount('(?1)(?t>=10)(?2)')(time, event_name='view', event_name='checkout') AS count
    FROM event
    GROUP BY user
)
WHERE count > 0;







-- View, add to cart, checkout ( have (?n).* and not .*)
SELECT user,
       sequenceCount('(?1)(?2)(?3)')(time, event_name = 'view', event_name = 'add_to_cart',
                     event_name = 'checkout') AS count
FROM event
GROUP BY user;


SELECT user,
       sequenceCount('(?1).*(?2).*(?3).*')(time, event_name = 'view', event_name = 'add_to_cart',
                     event_name = 'checkout') AS count
FROM event
GROUP BY user
;







-- view, add to cart not checkout
-- SELECT * FROM (
--     SELECT user,
--            sequenceCount('(?1).*(?2).*(?3)')(time,event_name='view' ,event_name='add_to_cart', event_name='checkout') AS count
--     FROM event
--     GROUP BY user
-- )
-- WHERE count > 0;
SELECT inner.user,
       outer.count - inner.count as count
FROM
(
    SELECT user,
           sequenceCount('(?1).*(?2).*(?3).*')(time, event_name = 'view', event_name = 'add_to_cart',
                         event_name = 'checkout') AS count
    FROM event
    GROUP BY user
) as inner
    INNER JOIN
(
    SELECT user,
           sequenceCount('(?1).*(?2).*')(time,event_name='view' ,event_name='add_to_cart') AS count
    FROM event
    GROUP BY user
) as outer
    ON inner.user = outer.user
;

-- identify, view add to cart, checkout
SELECT * FROM (
    SELECT user,
           sequenceCount('(?1)(?2)(?3)')(time, event_name='identify', event_name='view', event_name='add_to_cart', event_name='checkout') AS count
    FROM event
    GROUP BY user
)
WHERE count > 0;



-- (identify, view, search)
SELECT * FROM (
    SELECT user,
           sequenceCount('(?1)(?2)(?3)')(time, event_name='identify', event_name='view', event_name='search') AS count
    FROM event
    GROUP BY user
)
WHERE count > 0;


-- identify 1 lan, view 2 lan, add to cart
SELECT * FROM (
    SELECT user,
           sequenceCount('(?1)(?2)(?2)(?3)')(time, event_name='identify', event_name='view', event_name='add_to_cart') AS count
    FROM event
    GROUP BY user
)
WHERE count > 0;



-- performed (identify, view) greaterThan 1
SELECT * FROM (
    SELECT user,
           sequenceCount('(?1).*(?2).*')(time, event_name='identify', event_name='view') AS count
    FROM event
    GROUP BY user
)
WHERE count > 1;



-- view greaterThanOrEqualTo 2
-- expect: u1
SELECT * FROM (
    SELECT user,
           count() AS count
    FROM event
    WHERE event_name = 'view'
    GROUP BY user
)
WHERE count >= 2;


-- identify lessThan 2
-- expect: all user except u1
-- SELECT * FROM (
--     SELECT user,
--            sequenceCount('(?1)(?2)')(time, event_name='identify', event_name!='identify') AS count
--     FROM event
--     GROUP BY user
-- )
-- WHERE count = 1;


SELECT * FROM (
    SELECT user,
           count() as count
    from event
    where event_name = 'identify'
    group by user
) WHERE count = 1

;




SELECT * FROM event order by user, time;

not i = ( all - (i >= 1))
i <= n = ((i>=1) - (i>=n))


--- not i

SELECT user,
       sequenceCount('(?1).*(?2).*(?3).*')(time, event_name = 'view', event_name = 'add_to_cart', event_name = 'checkout') AS inner,
       sequenceCount('(?1).*(?2).*')(time,event_name='view' ,event_name='add_to_cart') as outer,
        outer - inner as count
FROM event
GROUP BY user

;

------------identify 1 time and view lessThan 3 times

SELECT user,
       sequenceCount('(?1).*(?2)')(time, event_name = 'identify', event_name = 'view') AS count
FROM event
GROUP BY user