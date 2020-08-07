create table n (
    a Nullable(String),
    b String,
    c Int16,
    d Nullable(Int16),
    e Nullable(Array(String))
)engine = Log()

insert into n values (Null, 'str', 12, Null)

select * from n;
┌─a────┬─b───┬──c─┬────d─┐
│ ᴺᵁᴸᴸ │ str │ 12 │ ᴺᵁᴸᴸ │
└──────┴─────┴────┴──────┘


insert into n values ('chi', 'le', 0, Null)

select * from n; -- 2
select count(a) from n; -- 1
select count(b) from n; -- 2
select count(d) from n; --2



create table n1 (
    e Nullable(Array(String))
)engine = Log()
-- Nested type Array(String) cannot be inside Nullable type.
create table n1 (
    e Array(Nullable(String))
)engine = Log()
-- It's OK





-- A Nullable type field can’t be included in table indexes.
create table n2 (
    a Nullable(String),
    b String
)engine = MergeTree()
order by a
--DB::Exception: Sorting key cannot contain nullable columns.



--> Using Nullable almost always negatively affects performance