create table if not exists summtt(
    key UInt32,
    value UInt32,
    s_value String
)Engine = SummingMergeTree(value)
order by key

insert into summtt values
(1,1,'chi'), (1,2,'le'), (2,1,'van')

select key, sum(value), s_value from summtt group by key