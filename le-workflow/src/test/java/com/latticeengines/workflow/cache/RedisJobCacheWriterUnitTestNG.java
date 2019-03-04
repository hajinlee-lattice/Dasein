package com.latticeengines.workflow.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mockito.Mockito;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobCache;

public class RedisJobCacheWriterUnitTestNG {
    private static final int MAX_ATTEMPTS = 1;
    private static final long INITIAL_WAIT_MILLIS = 100;
    private static final double MULTIPLIER = 2.0;

    private RetryTemplate retryTemplate;
    private JobCacheWriter cacheWriter;

    @BeforeClass(groups = "unit")
    public void setUp() {
        retryTemplate = RetryUtils.getExponentialBackoffRetryTemplate(
                MAX_ATTEMPTS, INITIAL_WAIT_MILLIS, MULTIPLIER, Collections.singletonMap(Exception.class, true));
    }

    @Test(groups = "unit", dataProvider = "jobCacheProvider")
    public void getByWorkflowId(Long id, boolean includeDetails, JobCache cache) {
        mockMultiGet(cache);
        JobCache result = cacheWriter.getByWorkflowId(id, includeDetails);
        if (cache == null) {
            Assert.assertNull(result);
        } else {
            assertEqual(result, cache);
        }
    }

    @Test(groups = "unit", dataProvider = "jobCacheListProvider")
    public void getByWorkflowIds(List<Long> ids, boolean includeDetails, List<JobCache> caches) {
        mockMultiGet(caches);
        List<JobCache> results = cacheWriter.getByWorkflowIds(ids, includeDetails);
        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), ids.size());
        for (int i = 0; i < ids.size(); i++) {
            if (caches.get(i) == null) {
                Assert.assertNull(results.get(i));
            } else {
                assertEqual(results.get(i), caches.get(i));
            }
        }
    }

    @Test(groups = "unit", dataProvider = "jobCacheUpdateTimesProvider")
    public void getUpdateTimes(List<Long> ids, boolean includeDetails, List<Long> updateTimes) {
        List<Object> objects = new ArrayList<>(updateTimes);
        mockMultiGet(objects, retryTemplate);
        List<Long> results = cacheWriter.getUpdateTimeByWorkflowIds(ids, includeDetails);
        Assert.assertEquals(results, updateTimes);
    }

    @DataProvider(name = "jobCacheProvider")
    private Object[][] provideJobCache() {
        return new Object[][] {
                { 123L, true, newJobCache(123L) },
                { 123L, false, newJobCache(123L) },
                { 456L, true, null },
                { 456L, false, null }
        };
    }

    @DataProvider(name = "jobCacheListProvider")
    private Object[][] provideJobCacheList() {
        return new Object[][] {
                { Arrays.asList(123L, 456L, 789L), true, newJobCaches(123L, 456L, 789L) },
                { Arrays.asList(123L, 456L, 789L), true, newJobCaches(null, 456L, null) },
                { Arrays.asList(123L, 456L, 789L), true, newJobCaches(null, null, null) },
                { Arrays.asList(123L, 456L, 789L), true, newJobCaches(null, null, 789L) },
                { Arrays.asList(123L, 456L, 789L), true, newJobCaches(123L, 456L, null) },
                { Arrays.asList(123L, 456L), true, newJobCaches(null, 456L) },
                { Collections.singletonList(123L), true, newJobCaches(new Long[] { null }) },
                { Collections.singletonList(123L), true, newJobCaches(123L) }
        };
    }

    @DataProvider(name = "jobCacheUpdateTimesProvider")
    public Object[][] provideJobCacheUpdateTimes() {
        return new Object[][] {
                { Arrays.asList(123L, 456L, 789L ), true, Arrays.asList(123456789L, 23456L, 654321L) },
                { Arrays.asList(123L, 456L, 789L ), true, Arrays.asList(null, 23456L, null) },
                { Arrays.asList(123L, 456L, 789L ), true, Arrays.asList(null, 23456L, 654321L) },
                { Arrays.asList(123L, 456L, 789L ), true, Arrays.asList(null, 23456L, null) },
                { Arrays.asList(123L, 456L, 789L ), true, Arrays.asList(12345L, 23456L, null) },
                { Arrays.asList(123L, 456L, 789L ), true, Arrays.asList(null, null, null) },
        };
    }

    private List<JobCache> newJobCaches(Long... ids) {
        return Arrays.stream(ids).map(this::newJobCache).collect(Collectors.toList());
    }

    private JobCache newJobCache(Long id) {
        if (id == null) {
            return null;
        }
        Job job = new Job();
        job.setId(id);
        return new JobCache(job, System.currentTimeMillis());
    }

    private void assertEqual(JobCache cache, JobCache expected) {
        Assert.assertNotNull(cache);
        Assert.assertNotNull(cache.getJob());
        Assert.assertEquals(cache.getJob(), expected.getJob());
        Assert.assertEquals(cache.getUpdatedAt(), expected.getUpdatedAt());
    }

    private void mockMultiGet(List<JobCache> jobCaches) {
        List<Object> objects = jobCaches.stream().flatMap(cache -> {
            if (cache == null) {
                // cache entry not exist
                return Stream.of(null, null);
            } else {
                return Stream.of(cache.getJob(), cache.getUpdatedAt());
            }
        }).collect(Collectors.toList());
        mockMultiGet(objects, retryTemplate);
    }

    private void mockMultiGet(JobCache jobCache) {
        if (jobCache != null) {
            mockMultiGet(Arrays.asList(jobCache.getJob(), jobCache.getUpdatedAt()), retryTemplate);
        } else {
            mockMultiGet(Arrays.asList(null, null), retryTemplate);
        }
    }

    @SuppressWarnings("unchecked")
    private void mockMultiGet(List<Object> objects, RetryTemplate template) {
        RedisTemplate<String, Object> redisTemplate = Mockito.mock(RedisTemplate.class);
        ValueOperations<String, Object> ops = Mockito.mock(ValueOperations.class);
        Mockito.when(redisTemplate.opsForValue()).thenReturn(ops);
        Mockito.when(ops.multiGet(Mockito.anyCollection())).thenReturn(objects);
        cacheWriter = new RedisJobCacheWriter(redisTemplate, template);
    }
}
