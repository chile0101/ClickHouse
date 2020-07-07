ASELECT arrayMap(x-> x+ 2, [1,2,3]) 

SELECT arrayMap((x,y) -> (x,y) , [1,2,3],[4,5,6])

----------------------
-- arrayFilter(func, arr1, …) 
-- Returns an array containing only the elements in arr1 for which func returns something other than 0.

SELECT arrayFilter(x -> x LIKE '%World%',['Hello','abc World']) AS res

SELECT 
    arrayFilter(
        (i, x) -> x LIKE '%World%',
        arrayEnumerate(arr), 
        ['Hello','abc World'] AS arr
    ) AS res

---------------------
-- replace arr1[i] by arr1[i - 1] if func returns 0.The first element of arr1 will not be replaced.

SELECT arrayFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
--                                   [1, 1,    3, 11, 12, 12,   12,  5,  6, 14, 14, 14]

----------------------
-- replace arr1[i] by arr1[i + 1] if func returns 0. The last element of arr1 will not be replaced.

SELECT arrayReverseFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
                                            -- [1, 3,    3, 11, 12, 5,  , 5,    5, 6, 14, null, null]


-----------------------

SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [0, 1, 0, 0, 1]) AS res
                           --  [[1],[2,3,4],[5]]


---------------------------


SELECT arrayReverseSplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
                    --                 [[1],[2,3,4],[5]]


---------------------------
SELECT arrayCount([1,2,3,4])

---------------------------
SELECT arrayExists((x,y) -> y, [1,2,3],[1,1,1])