SELECT COUNT(1)
FROM test.public.loadtesteventtable2_full_10m A
JOIN test.public.loadtesteventtable2_10m B
ON A.id = B.id
WHERE 