CREATE TABLE queries(
    Period Date,
    QueryID UInt32,
    Fingerprint String,
    Errors Nested(
        ErrorCode String,
        ErrorCnt UInt32
    )
)Engine=MergeTree(Period,QueryID,8192)

INSERT INTO queries VALUES ('2017-08-17',5,'SELECT foo FROM bar WHERE id=?',['1220','1230','1212'],[5,6,2])

INSERT INTO queries VALUES ('2017-08-18',5,'SELECT foo FROM bar WHERE id=?',['1220','1240','1258'],[3,2,1])

SELECT * FROM queries ARRAY JOIN Errors

SELECT 
    QueryID, 
    Errors.ErrorCode,
    SUM(Errors.ErrorCnt)
FROM queries 
ARRAY JOIN Errors 
GROUP BY 
    QueryID,
    Errors.ErrorCode 


SELECT 
    QueryID,
    groupArray((ecode, cnt))
FROM 
(
    SELECT 
        QueryID,
        ecode,
        sum(ecnt) AS cnt 
    FROM queries
    ARRAY JOIN 
        Errors.ErrorCode AS ecode,
        Errors.ErrorCnt AS ecnt 
    GROUP BY 
        QueryID,
        ecode
)
GROUP BY QueryID