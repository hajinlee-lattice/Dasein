SELECT COUNT(1)
FROM test.public.loadtesteventtable2 A
JOIN test.public.loadtesteventtable2_10m B
ON A.id = B.id
WHERE A.activity_count_click_email = 1 OR B.alexaviewsperuser = 1