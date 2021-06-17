select count(*), now() from caresoft_kgvietnam.contacts;
-- 162675,2021-06-10 10:01:00.566471


select max(updated_at) from caresoft_kgvietnam.contacts where updated_at < '2020-09-01 00:00:00';
select max(updated_at) from caresoft_kgvietnam.contacts where updated_at < '2020-05-01 00:00:00';
-- 2020-06-24 23:39:58.000000