show tables;

with ['s1','s1','s2','s2','s3'] as arr_1
    ['s3','s1','s1','s2','s2'] as arr_2,
     arrayEnumerate(arr_1) as idx
select arrayFilter()