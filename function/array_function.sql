-- empty
select empty([]); -- 1
select empty([1,2]); -- 0
-- length
-- range
select range(3);-- [0,1,2]
select range(5, 12, 2); -- [5,7,9,11]
select range(1, 10, 4); -- [1,5,9]

-- array(x1,...), operator [x1,...]
with
    array(1,2,3) as arr
select arr; -- [1,2,3]

-- arrayConcat
select arrayConcat([1,2,3], [5,6]); -- [1,2,3,5,6]

-- arrayElement(arr, n), operator [n]: index begin 1
with
    [1,2,3] as arr
select arr[0], arr[1], arr[4]; -- 0, 1, 0
-- has
with
    [1,2,3] as arr
select has(arr, 1), has(arr, 5); -- 1, 0

-- hasAll
select hasAll([1,2,3], [1]) ;-- 1
-- hasAny: check whether two arrays have intersectionby some element
with
    [1,2,3,4,5] as arr1,
     [4,5,6,7] as arr2
select hasAny(arr1, arr2) -- 1
;
-- hasSubStr: whether all the elements of array2 appear in array1 in the same exact order.
select hasSubstr([1,2,3,4],[1,2]);
-- indexOf(arr, x)
select indexOf([1,2,3,Null], Null); -- return 4
select indexOf([1,2,3,Null], 5); -- return 0
select [1,2,3,Null][0] ; -- return Null
-- arrayCount
select arrayCount([1,2,3,4, 5]); -- return 5, number of elements non-zero
select arrayCount(x -> x< 5, [1,2,3,4,5,6]); -- 4, It is high order function
-- countEqual = arrayCount(elem -> elem = x, arr)
-- arrayEnumerate(arr):
    --normally used with ARRAY JOIN. It allows counting something just once for each array after applying ARRAY JOIN
select arrayEnumerate([3,3,7,9,12]); -- [1,2,3,4]

create table array_test(
    name String,
    arr Array(Int16)
)Engine = Log();
insert into array_test values
('chi', [1,2,3]),
('le',[3,4,5]),
('van',[4,5,6]);
insert into array_test values
('chi', [1,2,3,4,5,6]);
insert into array_test values
('huynh', []);

insert into array_test values
('aaa', [1,1,2,2]);
select * from array_test;

select
    count() as Reaches,
    countIf(num=1) as Hits
from array_test
array join arr, arrayEnumerate(arr) as num
;

select arr,
       arrayEnumerateUniq(arr)
from array_test;

select sum(length(arr)) as reaches,
       count() as hits
from array_test
where notEmpty(arr);
┌─Reaches─┬──Hits─┐
│   15    │ 4     │
└─────────┴───────┘
can also be used in higher-order functions.
For example, you can use it to get array indexes for elements that match a condition.

with
    [1,2,3] as arr
select arrayFilter( x, idx -> arr[idx] =2, arr, arrayEnumerate(arr));

-- arrayEnumerateUniq(arr, ...)
select arrayEnumerateUniq([10, 20, 10, 30]); --[1,1,2,1]


create table array_test_1(
    id String,
    name String,
    arr Array(Int16)
)Engine = Log();

insert into array_test_1 values
('tiki', 'view', [1,2,3,4]),
('tiki', 'checkout', [3,4,5]),
('shope','view',[6,7,8]);

insert into array_test_1 values
('tiki', 'out', [1,2,2,2]);

insert into array_test_1 values
('aaa', 'kaka', [1,2,2,2]);

select * from array_test_1;

select *
from array_test_1 array join arr, arrayEnumerate(arr) as a;

select id,
       count() as users,
       countIf(num=1) as num_event_type
from array_test_1
array join arr, arrayEnumerateUniq(arr) as num
group by id
;

tiki,7,2
shope,3,1

SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2],
                          [1, 1, 2, 1, 1, 2]) AS res;


drop table array_test;
drop table array_test_1;

-- array join
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);

insert into arrays_test values
('World',[6]);

select *
from arrays_test
array join arr;

-- left array join
select *
from arrays_test
left array join arr;

select s, arr ,arr_alias
from arrays_test array join arr as arr_alias;

select s, arr_external
from arrays_test
array join [1,2,3] as arr_external;




select s, arr, a, num, numuniq, mapped
from arrays_test
left array join arr as a, arrayEnumerate(arr) as num,arrayEnumerateUniq(s) as numuniq, arrayMap(x -> x+1, arr) as mapped;

select
    count() as reaches,
    countIf(num = 1) as hits
from arrays_test
array join arr, arrayEnumerate(arr) as num;


select countIf(numuniq = 1)
from (
      select s, arr, a, num, numuniq, mapped
      from arrays_test
           array join
           arr as a,
           arrayEnumerate(arr) as num,
           arrayEnumerateUniq(arr) as numuniq,
           arrayMap(x -> x + 1, arr) as mapped
);

create table enumuniq(
    id Array(Int16),
    value Array(Int16),
    sign String
)ENGINE = Log();
insert into enumuniq values
([1], [2,3,4],'s1'),
([2], [2,3,4], 's2'),
([2,3],[1,2,3], 's2'),
([3,4],[4,5,6], 's3');

insert into enumuniq values
([1], [2,3,4],'s1'),
([1], [2,3,4],'s1'),
([1], [2,3,4,4],'s1'),
 ([1], [2,3,4,4],'s1');

insert into enumuniq values
([1], [2,3,4,5],'s1'),
([2], [2,3,4,5,5],'s2'),

select * from enumuniq;

select
       value, a, value_enum, value_uniq
from enumuniq array join value as a, arrayEnumerate(value) as value_enum, arrayEnumerateUniq(value) as value_uniq;

"[2,3,4]",2,1,1
"[2,3,4]",3,2,1
"[2,3,4]",4,3,1
"[2,3,4]",2,1,1
"[2,3,4]",3,2,1
"[2,3,4]",4,3,1
"[1,2,3]",1,1,1
"[1,2,3]",2,2,1
"[1,2,3]",3,3,1
"[4,5,6]",4,1,1
"[4,5,6]",5,2,1
"[4,5,6]",6,3,1
"[2,3,4]",2,1,1
"[2,3,4]",3,2,1
"[2,3,4]",4,3,1
"[2,3,4]",2,1,1
"[2,3,4]",3,2,1
"[2,3,4]",4,3,1
"[2,3,4,4]",2,1,1
"[2,3,4,4]",3,2,1
"[2,3,4,4]",4,3,1
"[2,3,4,4]",4,4,2
"[2,3,4,4]",2,1,1
"[2,3,4,4]",3,2,1
"[2,3,4,4]",4,3,1
"[2,3,4,4]",4,4,2
"[2,3,4,5]",2,1,1
"[2,3,4,5]",3,2,1
"[2,3,4,5]",4,3,1
"[2,3,4,5]",5,4,1
"[2,3,4,5,5]",2,1,1
"[2,3,4,5,5]",3,2,1
"[2,3,4,5,5]",4,3,1
"[2,3,4,5,5]",5,4,1
"[2,3,4,5,5]",5,5,2






select
    id, a, enum_id, enum_uniq_id
from enumuniq array join id as a, arrayEnumerate(id) as enum_id, arrayEnumerateUniq(id) as enum_uniq_id;


-- Ap dung vao project coi :))







-- array join with nested
    ;



select tenant_id,
      anonymous_id,
     `identity.keys`,
       `identity.vals`
from  user_profile array join identity
where tenant_id = 1 and anonymous_id = 'a0' and `identity.keys` in ['email','user_id'];





-- arrayDifference
select arrayDifference([1,2,3])