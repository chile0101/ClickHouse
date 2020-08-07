CREATE TABLE test.table_for_dict (
  key_column UInt64,
  third_column String
)
ENGINE = MergeTree()
ORDER BY key_column;

insert into test.table_for_dict values
(1,'chi')
insert into test.table_for_dict values
(1,'chi 2')
insert into test.table_for_dict values
(1,'chi 3','le 3')
insert into table_for_dict values
(2,'tho','huynh')

INSERT INTO table_for_dict
    select  number,
            concat('Third value ', toString(number)),
            concat('Four value ', toString(number))
    from numbers(1000000);


-- Creates a new external dictionary with given structure, source, layout and lifetime.
-- DDL: Data Definition Language

CREATE DICTIONARY ndict(
  key_column UInt64 DEFAULT 0,
  third_column String DEFAULT 'third default'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST '0.0.0.0' PORT 9000 USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'test'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(HASHED());



select dictGet('dict.ndict', 'third_column', toUInt64(1))
select dictGet ('dict.ndict', 'four_column', toUInt64(2))



--- join with dictionary
-- join table
select *
from numbers(10) as n
join ndict on key_column = number
-- 0.004

-- key-value -> faster
select number,
        dictGet('test.ndict', 'third_column', number)
from numbers(10)
-- 0.001
