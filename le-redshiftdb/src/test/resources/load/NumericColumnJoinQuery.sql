SELECT COUNT(1)
FROM test.public.loadtesteventtable A
JOIN test.public.loadtesteventtable_small B
ON A.id = B.id
WHERE A.activity_count_click_email&1 = 1 OR B.alexaviewsperuser&2 = 1
