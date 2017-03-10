SELECT COUNT(1)
FROM test.public.loadtesteventtable2
WHERE employees_total&1 = 1  OR activity_count_interesting_moment_webinar&2 = 1
