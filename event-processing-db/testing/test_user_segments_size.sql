


select *
from user_segments_final;

select * from segment_users_final;

insert into user_segments values
(1,'a1',['s1','s2','s3','s4'], now())
;
insert into segment_users values
(1,'s1', ['a1','a2','a3','a4','a5'], now())

insert into user_segments values
(1,'a1',['s1','s2','s3','s4','s5','s6','s7','s8','s8','s9','s10','s11'], now())

insert into segment_users values
(1,'s3', ['a1'], now()),
(1,'s4', ['a1'], now())
;

insert into segment_users values
(1,'s3', ['a1'], now()),
(1,'s4', ['a1'], now())


insert into segment_users values
(1,'s5', ['a1'], now()),
(1,'s6', ['a1'], now()),
(1,'s7', ['a1'], now()),
(1,'s8', ['a1'], now()),
(1,'s9', ['a1'], now()),
(1,'s10', ['a1'], now()),

insert into segment_users values
(1,'s11', ['a1'], now()),

    SELECT
        u.tenant_id,
        u.anonymous_id,
        s.segment_id as segment,
        s.segment_size AS segment_size
    FROM
    (
        SELECT *
        FROM
        (
            SELECT
                tenant_id,
                anonymous_id,
                argMaxMerge(segments) AS segments,
                max(at_final) AS at
            FROM user_segments_final
            WHERE tenant_id = 1 AND anonymous_id = 'a1'
            GROUP BY
                tenant_id,
                anonymous_id
        )
        ARRAY JOIN segments
    ) AS u
    INNER JOIN
    (
        SELECT
            tenant_id,
            segment_id,
            length(argMaxMerge(users)) AS segment_size
        FROM segment_users_final
        GROUP BY
            tenant_id,
            segment_id
    ) AS s ON (u.tenant_id = s.tenant_id) AND (u.segments = s.segment_id)
;
select toUnixTimestamp(toDateTime('1997-09-05 15:33:30'))
;
select toTimeZone("")