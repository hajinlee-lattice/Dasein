package com.latticeengines.workflow.cache;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobListCache;

@Component("tenantJobIdListCacheWriter")
public class RedisTenantJobIdListCacheWriter implements TenantJobIdListCacheWriter {

    private static final Map<Class<? extends Throwable>, Boolean> RETRY_EXCEPTIONS = new HashMap<>();
    private static final String CACHE_KEY_PREFIX = CacheName.Constants.JobsCacheName;
    private static final String DELIMITER = ":";
    private static final String JOB_ID_LIST_KEY = "JOB_ID_LIST";

    static {
        RETRY_EXCEPTIONS.put(RedisConnectionFailureException.class, true);
        RETRY_EXCEPTIONS.put(RedisSystemException.class, true);
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

    public RedisTenantJobIdListCacheWriter() {
    }

    public RedisTenantJobIdListCacheWriter(RedisTemplate<String, Object> redisTemplate, RetryTemplate retryTemplate) {
        Preconditions.checkNotNull(redisTemplate);
        Preconditions.checkNotNull(retryTemplate);
        this.redisTemplate = redisTemplate;
        this.retryTemplate = retryTemplate;
    }

    @Override
    public JobListCache get(Tenant tenant) {
        check(tenant);
        final String key = getKey(tenant);
        return retryTemplate.execute(ctx -> {
            Object obj = redisTemplate.opsForValue().get(key);
            if (!(obj instanceof JobIdListCache)) {
                // null or invalid type both considered cache miss
                return null;
            }

            return ((JobIdListCache) obj).getJobListCache();
        });
    }

    @Override
    public void set(Tenant tenant, List<Job> jobs) {
        check(tenant);
        Preconditions.checkNotNull(jobs);
        if (jobs.isEmpty()) {
            return;
        }

        final String key = getKey(tenant);
        retryTemplate.execute(ctx -> {
            List<JobIdCache> idCaches = jobs
                    .stream()
                    .filter(this::containsIdField)
                    .map(JobIdCache::new)
                    .collect(Collectors.toList());
            JobIdListCache listCache = new JobIdListCache(idCaches, System.currentTimeMillis());
            redisTemplate.opsForValue().set(key, listCache);
            return null;
        });
    }

    @Override
    public void clear(Tenant tenant) {
        check(tenant);
        retryTemplate.execute(ctx -> redisTemplate.delete(getKey(tenant)));
    }

    @PostConstruct
    private void setup() {
        retryTemplate = RetryUtils.getExponentialBackoffRetryTemplate(
                maxAttempts, initialWaitMsec, multiplier, RETRY_EXCEPTIONS);
    }

    private void check(Tenant tenant) {
        Preconditions.checkNotNull(tenant);
        Preconditions.checkNotNull(tenant.getPid());
    }

    /*
     * Cache key format: {PREFIX}:{NAMESPACE}:$JOB_ID_LIST_KEY:$PID
     */
    private String getKey(@NotNull Tenant tenant) {
        // only using pid for now
        return StringUtils.collectionToDelimitedString(
                Arrays.asList(CACHE_KEY_PREFIX, namespace, JOB_ID_LIST_KEY, tenant.getPid()), DELIMITER);
    }

    private boolean containsIdField(Job job) {
        if (job == null) {
            return false;
        }

        return job.getPid() != null || job.getId() != null || job.getApplicationId() != null;
    }

    /*
     * internal objects for cache entry
     */
    @SuppressWarnings("unused")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private static class JobIdCache {
        Long pid;
        Long id;
        String applicationId;

        JobIdCache() {
        }

        public Long getPid() {
            return pid;
        }

        public void setPid(Long pid) {
            this.pid = pid;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getApplicationId() {
            return applicationId;
        }

        public void setApplicationId(String applicationId) {
            this.applicationId = applicationId;
        }

        /* transform to/from external representation */

        @JsonIgnore
        JobIdCache(Job job) {
            this.pid = job.getPid();
            this.id = job.getId();
            this.applicationId = job.getApplicationId();
        }

        @JsonIgnore
        public Job getJob() {
            Job job = new Job();
            job.setId(id);
            job.setPid(pid);
            job.setApplicationId(applicationId);
            return job;
        }
    }

    @SuppressWarnings("unused")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private static class JobIdListCache {
        List<JobIdCache> caches;
        Long updatedAt;

        JobIdListCache() {
        }

        public List<JobIdCache> getCaches() {
            return caches;
        }

        public void setCaches(List<JobIdCache> caches) {
            this.caches = caches;
        }

        public Long getUpdatedAt() {
            return updatedAt;
        }

        public void setUpdatedAt(Long updatedAt) {
            this.updatedAt = updatedAt;
        }

        JobIdListCache(List<JobIdCache> caches, Long updatedAt) {
            this.caches = caches;
            this.updatedAt = updatedAt;
        }

        @JsonIgnore
        public JobListCache getJobListCache() {
            JobListCache listCache = new JobListCache();
            if (caches != null) {
                listCache.setJobs(caches
                        .stream()
                        .filter(Objects::nonNull)
                        .map(JobIdCache::getJob)
                        .collect(Collectors.toList()));
            }
            listCache.setUpdatedAt(updatedAt);
            return listCache;
        }
    }
}
