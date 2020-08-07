CREATE TABLE user (
  userid UInt32,
  name String
) ENGINE=MergeTree
PARTITION BY tuple()
ORDER BY userid

CREATE TABLE download (
  when DateTime,
  userid UInt32,
  bytes UInt64
) ENGINE=MergeTree
PARTITION BY toYYYYMM(when)
ORDER BY (userid, when)

CREATE TABLE price (
  userid UInt32,
  price_per_gb Float64
) ENGINE=MergeTree
PARTITION BY tuple()
ORDER BY userid

-- targe
CREATE TABLE download_daily (
  day Date,
  userid UInt32,
  downloads UInt32,
  total_gb Float64,
  total_price Float64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (userid, day)

--------- mv 1 --------------------------join truoc khi group
CREATE MATERIALIZED VIEW download_daily_mv TO download_daily AS
SELECT
    day,
    userid,
    count() AS downloads,
    sum(gb) AS total_gb,
    sum(p) AS total_price
FROM
(
    SELECT
        toDate(when) AS day,
        userid,
        bytes / ((1024 * 1024) * 1024) AS gb,
        gb * price_per_gb AS p
    FROM download
    LEFT JOIN price ON download.userid = price.userid
)
GROUP BY
    day,
    userid
--0.005



-----------------mv2 group truoc khi join
CREATE MATERIALIZED VIEW download_daily_mv TO download_daily AS
SELECT
    day,
    userid,
    c AS downloads,
    total_byte / ((1024 * 1024) * 1024) AS total_gb,
    total_gb * price_per_gb
FROM
(
    SELECT
        toDate(when) AS day,
        userid,
        count() AS c,
        sum(bytes) AS total_byte
    FROM download
    GROUP BY
        day,
        userid
) AS d
LEFT JOIN price ON d.userid = price.userid
-- 0.005

---------------------------------------------> 2 merge view tuong duong nhau.

------------------ insert
insert into download values
('2020-01-01 00:00:00', 1,12800),
('2020-01-01 00:05:00', 1,10600),
('2020-01-01 00:12:06', 2,6800),
('2020-01-02 00:16:00', 1,800)

insert into download values
('2020-01-01 00:40:00', 1,1200),
('2020-01-01 00:45:00', 1,1250)

----------------

INSERT INTO price VALUES (25, 0.10), (26, 0.05), (27, 0.01);

INSERT INTO user VALUES (25, 'Bob'), (26, 'Sue'), (27, 'Sam');



insert into download
with
    (select groupArray(userid) from user) as user_ids
select
    now() + number * 60 as when,
    user_ids[(number%length(user_ids) + 1)] as user_id,
    rand()% 100000000 as bytes
from system.numbers
limit 5000


SELECT
  when AS when,
  user.userid AS userid,
  user.name AS name,
  bytes AS bytes
FROM download RIGHT JOIN user ON (download.userid = user.userid)



