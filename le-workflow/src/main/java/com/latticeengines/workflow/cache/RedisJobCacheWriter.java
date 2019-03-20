package com.latticeengines.workflow.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobCache;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

@Component("jobCacheWriter")
public class RedisJobCacheWriter implements JobCacheWriter {

    private static final Map<Class<? extends Throwable>, Boolean> RETRY_EXCEPTIONS = new HashMap<>();
    private static final String CACHE_KEY_PREFIX = CacheName.Constants.JobsCacheName;
    private static final String DELIMITER = ":";
    private static final String DETAIL_KEY = "detail";
    private static final String UPDATE_TIME_KEY = "update_time";
    private static final int CLEAR_ALL_BATCH_SIZE = 50;

    static {
        RETRY_EXCEPTIONS.put(RedisConnectionFailureException.class, true);
        RETRY_EXCEPTIONS.put(RedisSystemException.class, true);
    }

    private enum CacheType {
        WORKFLOW_ID
    }

    @Value("${proxy.retry.initialwaitmsec:500}")
    private long initialWaitMsec;
    @Value("${proxy.retry.multiplier:2}")
    private double multiplier;
    @Value("${proxy.retry.maxattempts:5}")
    private int maxAttempts;
    @Value("${workflow.jobs.cache.namespace:default}")
    private String namespace;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private RetryTemplate retryTemplate;

    public RedisJobCacheWriter() {

    }

    public RedisJobCacheWriter(RedisTemplate<String, Object> redisTemplate, RetryTemplate retryTemplate) {
        Preconditions.checkNotNull(retryTemplate);
        Preconditions.checkNotNull(redisTemplate);
        this.retryTemplate = retryTemplate;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public JobCache getByWorkflowId(@NotNull Long workflowId, boolean includeDetails) {
        Preconditions.checkNotNull(workflowId);
        List<JobCache> caches = getByWorkflowIds(Collections.singletonList(workflowId), includeDetails);
        Preconditions.checkNotNull(caches);
        Preconditions.checkArgument(caches.size() == 1);
        return caches.get(0);
    }

    @Override
    public List<JobCache> getByWorkflowIds(@NotNull List<Long> workflowIds, boolean includeDetails) {
        check(workflowIds);
        return retryTemplate.execute(ctx -> {
            List<String> keys = workflowIds
                    .stream()
                    .flatMap(id -> {
                        JobCacheKey key = getJobCacheKeyByWorkflowId(id, includeDetails);
                        return Stream.of(key.jobKey, key.updateTimeKey);
                    })
                    .collect(Collectors.toList());
            List<Object> objects = redisTemplate.opsForValue().multiGet(keys);
            Preconditions.checkNotNull(objects);
            Iterator<Object> iterator = objects.iterator();
            return workflowIds
                    .stream()
                    .map(id -> getJobCache(iterator))
                    .collect(Collectors.toList());
        });
    }

    @Override
    public List<Long> getUpdateTimeByWorkflowIds(@NotNull List<Long> workflowIds, boolean includeDetails) {
        check(workflowIds);
        List<String> updateTimeKeys = workflowIds
                .stream()
                .map(id -> getJobCacheKeyByWorkflowId(id, includeDetails).updateTimeKey)
                .collect(Collectors.toList());
        List<Object> objects = retryTemplate.execute((ctx) -> redisTemplate.opsForValue().multiGet(updateTimeKeys));
        Preconditions.checkNotNull(objects);
        return objects.stream().map(this::getUpdateTime).collect(Collectors.toList());
    }

    @Override
    public void setUpdateTimeByWorkflowIds(List<Long> workflowIds, boolean includeDetails, long timestamp) {
        check(workflowIds);
        Map<String, Object> reqParams = workflowIds
                .stream()
                .map(id -> getJobCacheKeyByWorkflowId(id, includeDetails).updateTimeKey)
                .collect(Collectors.toMap(key -> key, key -> timestamp));
        multiSet(reqParams);
    }

    @Override
    public void put(Job job, boolean includeDetails) {
        check(job);
        put(Collections.singletonList(job), includeDetails);
    }

    @Override
    public void put(List<Job> jobs, boolean includeDetails) {
        Preconditions.checkNotNull(jobs);
        Map<String, Object> reqParams = jobs.stream()
                // ignore invalid jobs
                .filter(job -> job != null && job.getId() != null)
                .flatMap(job -> {
                    JobCacheKey key = getJobCacheKeyByWorkflowId(job.getId(), includeDetails);
                    return Stream.of(Pair.of(key.jobKey, job), Pair.of(key.updateTimeKey, System.currentTimeMillis()));
                })
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        multiSet(reqParams);
    }

    @Override
    public void clear(Job job, boolean includeDetails) {
        Preconditions.checkNotNull(job);
        JobCacheKey key = getJobCacheKeyByWorkflowId(job.getId(), includeDetails);
        retryTemplate.execute((ctx) -> redisTemplate.delete(Arrays.asList(key.jobKey, key.updateTimeKey)));
    }

    @Override
    public int clearAll() {
        // get all keys start with cache prefix, this can be slow
        String ptn = String.format("%s*", CACHE_KEY_PREFIX);
        Set<String> keys = redisTemplate.keys(ptn);
        if (CollectionUtils.isEmpty(keys)) {
            return 0;
        }

        List<String> keyList = new ArrayList<>(keys);
        int nKeys = keyList.size();
        // delete items in batches to prevent sending to much bytes at once. DEL
        // operation should be fast.
        List<List<String>> keysInBatch = Lists.partition(keyList, CLEAR_ALL_BATCH_SIZE);
        keysInBatch.forEach(this::deleteAll);
        return nKeys;
    }

    private void deleteAll(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return;
        }

        retryTemplate.execute(ctx -> redisTemplate.delete(keys));
    }

    private void multiSet(Map<String, Object> reqParams) {
        retryTemplate.execute((ctx) -> {
            redisTemplate.opsForValue().multiSet(reqParams);
            return null;
        });
    }

    /*
     * Retrieve job cache entry from an iterator with interleaved job cache entry and cache update time
     */
    private JobCache getJobCache(Iterator<Object> iterator) {
        Job job = getJob(iterator);
        if (job == null) {
            // cache miss if the job key does not have value
            if (iterator.hasNext()) {
                // discard next element
                iterator.next();
            }
            return null;
        }
        JobCache cache = new JobCache();
        cache.setJob(job);
        cache.setUpdatedAt(getUpdateTime(iterator.next()));
        return cache;
    }

    private Long getUpdateTime(Object obj) {
        try {
            if (obj instanceof String) {
                return Long.parseLong(obj.toString());
            } else if (obj instanceof Number) {
                return ((Number) obj).longValue();
            }
        } catch (Exception e) {
            // do nothing, cache will be considered expired
        }
        return null;
    }

    private Job getJob(Iterator<Object> iterator) {
        if (iterator == null || !iterator.hasNext()) {
            return null;
        }
        Object obj = iterator.next();
        return (obj instanceof Job) ? (Job) obj : null;
    }

    @PostConstruct
    private void setup() {
        retryTemplate = RetryUtils.getExponentialBackoffRetryTemplate(
                maxAttempts, initialWaitMsec, multiplier, RETRY_EXCEPTIONS);
    }

    private JobCacheKey getJobCacheKeyByWorkflowId(long workflowId, boolean includeDetails) {
        String jobKey = getCacheKeyByWorkflowId(workflowId, includeDetails);
        String updateTimeKey = getCacheKeyByWorkflowId(workflowId, includeDetails, UPDATE_TIME_KEY);
        return new JobCacheKey(jobKey, updateTimeKey);
    }

    private String getCacheKeyByWorkflowId(Long workflowId, boolean includeDetails, String... suffixStrs) {
        WorkflowJob job = new WorkflowJob();
        job.setWorkflowId(workflowId);
        return getCacheKey(job, includeDetails, CacheType.WORKFLOW_ID, suffixStrs);
    }

    private String getCacheKey(WorkflowJob job, boolean includeDetails, CacheType type, String... suffixStrs) {
        String id;
        // only cache with workflow ID for now
        switch (type) {
            case WORKFLOW_ID:
                id = job.getWorkflowId().toString();
                break;
            default:
                throw new IllegalArgumentException("Invalid job cache type: " + type);
        }
        return getCacheKey(id, includeDetails, type, suffixStrs);
    }

    /*
     * Cache key format: {PREFIX}:{NAMESPACE}:{CACHE_TYPE}:{DETAIL?}:{JOB_ID}:{SUFFIX}
     */
    private String getCacheKey(String id, boolean includeDetails, CacheType type, String... suffixStrs) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(id);
        List<String> params = new ArrayList<>();
        Collections.addAll(params, CACHE_KEY_PREFIX, namespace, type.name());
        if (includeDetails) {
            params.add(DETAIL_KEY);
        }
        params.add(id);
        Collections.addAll(params, suffixStrs);
        return StringUtils.collectionToDelimitedString(params, DELIMITER);
    }

    /*
     * the input list and each ID cannot be null
     */
    private void check(List<Long> ids) {
        Preconditions.checkNotNull(ids);
        for (Long id : ids) {
            Preconditions.checkNotNull(id);
        }
    }

    private void check(Job job) {
        Preconditions.checkNotNull(job);
        Preconditions.checkNotNull(job.getId());
    }

    /**
     * Cache key holder
     */
    private static class JobCacheKey {
        final String jobKey;
        final String updateTimeKey;

        JobCacheKey(String jobKey, String updateTimeKey) {
            this.jobKey = jobKey;
            this.updateTimeKey = updateTimeKey;
        }
    }
}
