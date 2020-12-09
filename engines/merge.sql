select currentDatabase();
show tables ;

create table profile_final as profile_str_final ENGINE = Merge(currentDatabase(), 'profile_(str|num|arr)_final');
select * from profile_final;


drop table WatchLog_old;
CREATE TABLE WatchLog_old(
    date Date,
    UserId Int64,
    EventType String,
    Cnt Array(String)
)
ENGINE=MergeTree(date, (UserId, EventType), 8192);

INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', ['chi','le']);

DROP table WatchLog_new;
CREATE TABLE WatchLog_new(
    date Date,
    UserId Int64,
    EventType String,
    Cnt Array(Float32))
ENGINE=MergeTree PARTITION BY date ORDER BY (UserId, EventType) SETTINGS index_granularity=8192;
INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', [1,2,3]);

drop table WatchLog;
CREATE TABLE WatchLog as WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

select * from WatchLog;