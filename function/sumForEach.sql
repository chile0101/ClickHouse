id    units
 1    [1,1,1]
 2    [3,0,0]
 1    [5,3,7]
 3    [2,5,2]
 2    [3,2,6]


id    units
 1    [6,4,8]
 2    [6,2,6]
 3    [2,5,2]


SELECT id, sumForEach(units) units
FROM (
  /* emulate dataset */
  SELECT data.1 id, data.2 units
  FROM (
    SELECT arrayJoin([(1, [1,1,1]), (2, [3,0,0]), (1, [5,3,7]), (3, [2,5,2]), (2, [3,2,6])]) data))
GROUP BY id;


SELECT id, avgForEach(units) units
FROM (
  /* emulate dataset */
  SELECT data.1 id, data.2 units
  FROM (
    SELECT arrayJoin([(1, [1,1,1]), (2, [3,0,0]), (1, [5,3,7]), (3, [2,5,2]), (2, [3,2,6])]) data))
GROUP BY id