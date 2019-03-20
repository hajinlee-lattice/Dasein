package com.latticeengines.workflow.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;
import org.springframework.util.CollectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.service.ReportService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobCache;
import com.latticeengines.domain.exposed.workflow.JobListCache;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.workflow.cache.JobCacheWriter;
import com.latticeengines.workflow.cache.TenantJobIdListCacheWriter;
import com.latticeengines.workflow.core.LEJobExecutionRetriever;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.JobCacheService;
import com.latticeengines.workflow.exposed.util.WorkflowJobUtils;

@Service("jobCacheService")
public class JobCacheServiceImpl implements JobCacheService {

    private static final Logger log = LoggerFactory.getLogger(JobCacheServiceImpl.class);

    private static final int NUM_THREADS = 8;
    private static final int LOG_NUM_IDS_LIMIT = 20;
    private static final int CLEAR_TENANT_BATCH_SIZE = 50;

    @Value("${common.internal.app.url}")
    private String internalAppUrl;

    @Value("${common.microservice.url}")
    private String microserviceUrl;

    @Value("${workflow.jobs.cache.jobExpireTimeInMillis:30000}")
    private int runningJobExpireTimeInMillis; // expire time for job cache entries

    @Value("${workflow.jobs.cache.jobIdListExpireTimeInMillis:300000}")
    private int idListExpireTimeInMillis; // expire time for job id list cache entries

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private LEJobExecutionRetriever leJobExecutionRetriever;

    @Autowired
    private ReportService reportService;

    @Autowired
    private JobCacheWriter cacheWriter;

    @Autowired
    private TenantJobIdListCacheWriter jobIdListCacheWriter;

    private ExecutorService service = ThreadPoolUtils.getFixedSizeThreadPool("job-cache-service", NUM_THREADS);

    @Override
    public List<Job> getByTenant(Tenant tenant, boolean includeDetails) {
        Preconditions.checkNotNull(tenant);
        Preconditions.checkNotNull(tenant.getPid());

        JobListCache jobListCache = jobIdListCacheWriter.get(tenant);
        boolean cacheValid = true;
        if (jobListCache == null) {
            // cache miss
            log.info("JobIdListCache does not exist for tenant (PID={})", tenant.getPid());
            cacheValid = false;
        } else if (isCacheExpired(jobListCache)) {
            // cache expired
            log.info("JobIdListCache expired for tenant (PID={})", tenant.getPid());
            cacheValid = false;
        }

        Tenant prevTenant = MultiTenantContext.getTenant();
        try {
            MultiTenantContext.setTenant(tenant);
            if (!cacheValid) {
                jobListCache = new JobListCache();
                // TODO optimize to only get workflowId & pid
                List<Job> jobs = workflowJobEntityMgr.findAll()
                        .stream()
                        .map(workflowJob -> {
                            Job job = new Job();
                            job.setId(workflowJob.getWorkflowId());
                            job.setPid(workflowJob.getPid());
                            return job;
                        })
                        .collect(Collectors.toList());
                jobListCache.setJobs(jobs);
                if (!jobs.isEmpty()) {
                    log.info("Refresh JobIdListCache for tenant (PID={}), size={}", tenant.getPid(), jobs.size());
                }
                // refresh id list cache
                service.execute(() -> jobIdListCacheWriter.set(tenant, jobs));
            }
            return getTenantJobsInCache(jobListCache, includeDetails);
        } finally {
            MultiTenantContext.setTenant(prevTenant);
        }
    }

    @Override
    public Job getByWorkflowId(Long workflowId, boolean includeDetails) {
        Preconditions.checkNotNull(workflowId);
        List<Job> jobs = getByWorkflowIds(Collections.singletonList(workflowId), includeDetails);
        Preconditions.checkNotNull(jobs);
        return jobs.isEmpty() ? null : jobs.get(0);
    }

    @Override
    public List<Job> getByWorkflowIds(List<Long> workflowIds, boolean includeDetails) {
        check(workflowIds);

        Map<Long, JobCache> cacheJobMap = getCachedJobMap(workflowIds, includeDetails);
        // trigger async refresh for expired cache, still return stale data
        List<Long> expiredWorkflowIds = cacheJobMap.values()
                .stream()
                .filter(this::isCacheExpired)
                .map(cache -> cache.getJob().getId())
                .collect(Collectors.toList());
        if (!expiredWorkflowIds.isEmpty()) {
            log.info("Re-populating expired job caches, size={}, IDs={}, includeDetails={}",
                    expiredWorkflowIds.size(), getWorkflowIdsStr(expiredWorkflowIds), includeDetails);
        }
        putAsync(expiredWorkflowIds, false);

        // load jobs from datastore
        List<Long> missingJobIds = workflowIds
                .stream()
                .filter(id -> !cacheJobMap.containsKey(id))
                .collect(Collectors.toList());
        List<Job> missingJobs = getMissingJobs(missingJobIds, includeDetails);
        // make a defensive clone to avoid the instance returned from being modified
        List<Job> missingJobForPublish = missingJobs.stream() //
                .filter(Objects::nonNull) //
                .map(Job::shallowClone) //
                .collect(Collectors.toList());

        // populate for cache misses
        service.execute(() -> {
            if (missingJobs.isEmpty()) {
                return;
            }
            log.info("Populating missed job caches, size={}, IDs={}, includeDetails={}",
                    missingJobForPublish.size(),
                    getWorkflowIdsStr(missingJobForPublish.stream().map(Job::getId).collect(Collectors.toList())),
                    includeDetails);
            cacheWriter.put(missingJobForPublish, includeDetails);
        });

        // merge both lists
        Map<Long, Job> missingJobMap = missingJobs.stream().collect(Collectors.toMap(Job::getId, job -> job));
        return workflowIds
                .stream()
                .map(id -> cacheJobMap.containsKey(id) ?
                        // from cache
                        cacheJobMap.get(id).getJob() :
                        // from datastore
                        missingJobMap.get(id))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public void put(WorkflowJob workflowJob) {
        check(workflowJob);

        Job detailedJob = transform(workflowJob, true);
        Job job = transform(workflowJob, false);

        log.info("Refreshing job cache with workflow ID={}", workflowJob.getWorkflowId());
        cacheWriter.put(detailedJob, true);
        cacheWriter.put(job, false);
    }

    @Override
    public Future<?> putAsync(Long workflowId) {
        Preconditions.checkNotNull(workflowId);
        return putAsync(Collections.singletonList(workflowId));
    }

    @Override
    public Future<?> putAsync(List<Long> workflowIds) {
        check(workflowIds);
        return putAsync(workflowIds, true);
    }

    @Override
    public void evict(Tenant tenant) {
        if (tenant == null || tenant.getPid() == null) {
            // noop for invalid param
            return;
        }

        log.info("Evict JobIdListCache for tenant (PID={})", tenant.getPid());
        jobIdListCacheWriter.clear(tenant);
    }

    @Override
    public void evictByWorkflowIds(List<Long> workflowIds) {
        if (workflowIds == null) {
            return;
        }
        log.info("Evict job cache entries, size={}, jobIds={}", workflowIds.size(), getWorkflowIdsStr(workflowIds));
        workflowIds.forEach((workflowId) -> {
            if (workflowId == null) {
                return;
            }
            Job job = new Job();
            job.setId(workflowId);
            try {
                cacheWriter.clear(job, true);
                cacheWriter.clear(job, false);
            } catch (Exception e) {
                log.error("Failed to evict job cache entry, workflowID = {}", workflowId);
            }
        });
    }

    @Override
    public int deepEvict(Tenant tenant) {
        if (tenant == null || tenant.getPid() == null) {
            // noop for invalid tenant
            return 0;
        }

        JobListCache list = jobIdListCacheWriter.get(tenant);
        if (list == null) {
            return 0;
        }

        List<Long> workflowIds = list.getJobs() //
                .stream() //
                .filter(job -> job != null && job.getId() != null) //
                .map(Job::getId) //
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(workflowIds)) {
            return 0;
        }

        int nJobs = workflowIds.size();
        List<List<Long>> workflowIdsInBatch = Lists.partition(workflowIds, CLEAR_TENANT_BATCH_SIZE);
        // delete individual jobs
        workflowIdsInBatch.forEach(this::evictByWorkflowIds);
        // delete ID list cache
        jobIdListCacheWriter.clear(tenant);

        return nJobs;
    }

    @Override
    public int evictAll() {
        return cacheWriter.clearAll();
    }

    /*
     * MultiTenantContext should be already configured
     */
    private List<Job> getTenantJobsInCache(JobListCache jobListCache, boolean includeDetails) {
        if (jobListCache.getJobs().isEmpty()) {
            log.info("No jobs cached for tenant (PID={})", MultiTenantContext.getTenant().getPid());
            return Collections.emptyList();
        }
        // job object to store ids
        List<Job> jobs = jobListCache.getJobs();
        List<Long> workflowIds = jobs
                .stream()
                .filter(job -> job != null && job.getId() != null)
                .map(Job::getId)
                .collect(Collectors.toList());
        // pids of jobs that does not have workflowId
        List<Long> workflowPIds = jobs
                .stream()
                .filter(job -> job != null && job.getId() == null && job.getPid() != null)
                .map(Job::getPid)
                .collect(Collectors.toList());
        log.info("Retrieve jobs with workflow IDs in tenant (PID={}), size={}, jobIds={}",
                MultiTenantContext.getTenant().getPid(), workflowIds.size(), getWorkflowIdsStr(workflowIds));
        if (!workflowPIds.isEmpty()) {
            log.info("Retrieve jobs with only pid in tenant (PID={}), size={}, pids={}",
                    MultiTenantContext.getTenant().getPid(), workflowPIds.size(), getWorkflowIdsStr(workflowPIds));
        }
        Map<Long, Job> workflowIdJobMap = getByWorkflowIds(workflowIds, includeDetails)
                .stream()
                .collect(Collectors.toMap(Job::getId, job -> job));
        Map<Long, Job> pidJobMap = getPidJobMap(workflowPIds, includeDetails);
        return jobs.stream()
                .map(job -> {
                    if (job.getId() != null) {
                        return workflowIdJobMap.get(job.getId());
                    } else {
                        return pidJobMap.get(job.getPid());
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private Map<Long, Job> getPidJobMap(@NotNull List<Long> pids, boolean includeDetails) {
        if (pids.isEmpty()) {
            return Collections.emptyMap();
        }

        return workflowJobEntityMgr.findByWorkflowPids(pids)
                .stream()
                .filter(Objects::nonNull)
                .map(job -> transform(job, includeDetails))
                .collect(Collectors.toMap(Job::getPid, job -> job));
    }

    /*
     * if forceRefresh is
     * true: cache entry will be populated
     * false: only populate the cache entry if no one else is updating
     */
    private Future<?> putAsync(List<Long> workflowIds, boolean forceRefresh) {
        if (workflowIds.isEmpty()) {
            // ends immediately
            return CompletableFuture.completedFuture(null);
        }
        long timestamp = System.currentTimeMillis();
        log.info("Refreshing job cache, timestamp={}, forceRefresh={}, # of workflow IDs = {}, IDs={}",
                timestamp,
                forceRefresh,
                workflowIds.size(),
                getWorkflowIdsStr(workflowIds));
        if (!forceRefresh) {
            // when not forcing refresh, set the cache entry's update time
            // runnable only update if no other people's updating
            cacheWriter.setUpdateTimeByWorkflowIds(workflowIds, true, timestamp);
            cacheWriter.setUpdateTimeByWorkflowIds(workflowIds, false, timestamp);
        }
        return service.submit(new RefreshCacheRunnable(
                MultiTenantContext.getTenant(), workflowIds, forceRefresh ? null : timestamp));
    }

    private boolean isCacheExpired(JobCache cache) {
        if (cache.getUpdatedAt() == null) {
            return true;
        }
        Job job = cache.getJob();
        JobStatus status = job.getJobStatus();
        if (status == null || status.isTerminated()) {
            // job in terminal state does not expire
            return false;
        }

        long timestamp = System.currentTimeMillis();
        return timestamp - cache.getUpdatedAt() > runningJobExpireTimeInMillis;
    }

    private boolean isCacheExpired(JobListCache cache) {
        if (cache.getUpdatedAt() == null) {
            return true;
        }

        long timestamp = System.currentTimeMillis();
        return timestamp - cache.getUpdatedAt() > idListExpireTimeInMillis;
    }

    private List<Job> getMissingJobs(List<Long> missingJobIds, boolean includeDetails) {
        if (missingJobIds.isEmpty()) {
            return Collections.emptyList();
        }
        // entity manager cannot take empty list
        return workflowJobEntityMgr.findByWorkflowIds(missingJobIds)
                .stream()
                .map(workflowJob -> transform(workflowJob, includeDetails))
                .collect(Collectors.toList());
    }

    /*
     * return empty map if failed to retrieve from cache
     */
    private Map<Long, JobCache> getCachedJobMap(List<Long> workflowIds, boolean includeDetails) {
        Map<Long, JobCache> jobCacheMap = new HashMap<>();
        try {
            jobCacheMap = cacheWriter
                    .getByWorkflowIds(workflowIds, includeDetails)
                    .stream()
                    // consider a cache miss if the cache object is not valid
                    .filter(this::isCacheValid)
                    .collect(Collectors.toMap(cache -> cache.getJob().getId(), cache -> cache));
        } catch (Exception e) {
            String msg = String.format("Failed to retrieve job caches, size=%d, IDs=%s, includeDetails=%b",
                    workflowIds.size(), getWorkflowIdsStr(workflowIds), includeDetails);
            log.error(msg, e);
        }
        return jobCacheMap;
    }

    private String getWorkflowIdsStr(List<Long> workflowIds) {
        List<Long> truncatedIds = workflowIds.stream().limit(LOG_NUM_IDS_LIMIT).collect(Collectors.toList());
        if (truncatedIds.size() == workflowIds.size()) {
            return truncatedIds.toString();
        } else {
            return truncatedIds.toString() + "...";
        }
    }

    private void check(WorkflowJob workflowJob) {
        Preconditions.checkNotNull(workflowJob);
        Preconditions.checkNotNull(workflowJob.getWorkflowId());
    }

    private boolean isCacheValid(JobCache cache) {
        if (cache == null || cache.getJob() == null || cache.getJob().getId() == null) {
            return false;
        }

        Job job = cache.getJob();
        JobStatus status = job.getJobStatus();
        if ((status == JobStatus.FAILED || status == JobStatus.COMPLETED) && job.getEndTimestamp() == null) {
            // job terminated and has no end timestamp
            log.info("Job cache entry in terminal state but missing end timestamp found, considered a cache miss, ID={}",
                    job.getId());
            return false;
        }
        if (job.getTenantPid() == null && job.getTenantId() == null) {
            // old version of cache entries that does not have tenant info
            return false;
        }
        return true;
    }

    private void check(List<Long> ids) {
        Preconditions.checkNotNull(ids);
        for (Long id : ids) {
            Preconditions.checkNotNull(id);
        }
    }

    private Job transform(@Nullable WorkflowJob workflowJob, boolean includeDeatils) {
        if (workflowJob == null) {
            return null;
        }
        Tenant prevTenant = MultiTenantContext.getTenant();
        // set tenant for report retrieval
        try {
            MultiTenantContext.setTenant(workflowJob.getTenant());
            Job job = WorkflowJobUtils.assembleJob(reportService, leJobExecutionRetriever, getLpUrl(),
                    workflowJob, includeDeatils);
            if (workflowJob.getTenant() != null) {
                job.setTenantId(workflowJob.getTenant().getId());
                job.setTenantPid(workflowJob.getTenant().getPid());
            }
            return job;
        } finally {
            // restore the value before
            MultiTenantContext.setTenant(prevTenant);
        }
    }

    private String getLpUrl() {
        if (StringUtils.isBlank(internalAppUrl)) {
            return microserviceUrl;
        } else {
            return internalAppUrl;
        }
    }

    /*
     * Asynchronously refresh cache entries for target jobs.
     */
    private class RefreshCacheRunnable implements Runnable {
        private final Tenant tenant;
        private final List<Long> workflowIds;
        private final Long updateTime;

        RefreshCacheRunnable(@Nullable Tenant tenant, @NotNull List<Long> workflowIds, @Nullable Long updateTime) {
            Preconditions.checkNotNull(workflowIds);
            this.tenant = tenant;
            this.workflowIds = workflowIds.stream().filter(Objects::nonNull).collect(Collectors.toList());
            this.updateTime = updateTime;
        }

        @Override
        public void run() {
            Tenant prevTenant = MultiTenantContext.getTenant();
            try {
                List<Long> ids = new ArrayList<>(workflowIds);
                List<Long> detailIds = new ArrayList<>(workflowIds);
                if (updateTime != null) {
                    // check if the update time has changed, if other process has refreshed the cache entry,
                    // not refreshing again
                    List<Long> updateTimes = cacheWriter.getUpdateTimeByWorkflowIds(workflowIds, false);
                    List<Long> detailUpdateTimes = cacheWriter.getUpdateTimeByWorkflowIds(workflowIds, true);
                    ids = removeUpdatedIds(workflowIds, updateTimes, updateTime);
                    detailIds = removeUpdatedIds(workflowIds, detailUpdateTimes, updateTime);
                }

                MultiTenantContext.setTenant(tenant);
                if (tenant == null) {
                    log.debug("Set tenant to null in MultiTenantContext");
                } else {
                    log.debug("Set tenant to Tenant(PID={}, ID={})", tenant.getPid(), tenant.getId());
                }
                // merge IDs and dedup
                List<Long> jobIds = Stream.concat(ids.stream(), detailIds.stream()).distinct().collect(Collectors.toList());
                List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowIds(jobIds);
                Set<Long> idSet = new HashSet<>(ids);
                Set<Long> detailIdSet = new HashSet<>(detailIds);

                logUpdatedIds(jobIds, workflowIds);

                List<Job> jobs = workflowJobs
                        .stream()
                        .filter(job -> idSet.contains(job.getWorkflowId()))
                        .map(workflowJob -> transform(workflowJob, false))
                        .collect(Collectors.toList());
                List<Job> detailJobs = workflowJobs
                        .stream()
                        .filter(job -> detailIdSet.contains(job.getWorkflowId()))
                        .map(workflowJob -> transform(workflowJob, true))
                        .collect(Collectors.toList());
                List<Long> cachedJobIds = jobs.stream().map(Job::getId).collect(Collectors.toList());
                List<Long> cachedDetailJobIds = jobs.stream().map(Job::getId).collect(Collectors.toList());
                log.info("Populating job caches, updateTime={}, size={}, IDs={}",
                        updateTime, cachedJobIds.size(), getWorkflowIdsStr(cachedJobIds));
                log.info("Populating detail job caches, updateTime={}, size={}, IDs={}",
                        updateTime, cachedDetailJobIds.size(), getWorkflowIdsStr(cachedDetailJobIds));
                cacheWriter.put(jobs, false);
                cacheWriter.put(detailJobs, true);
            } catch (Exception e) {
                // clear cache just to be safe
                evictByWorkflowIds(workflowIds);
                if (e instanceof TransactionException) {
                    // something wrong transaction manager, probably main thread terminated
                    log.error("Failed to refresh job caches caused by transaction manager, updateTime={}, size={}, IDs={}",
                            updateTime, workflowIds.size(), getWorkflowIdsStr(workflowIds));
                    return;
                }
                log.error(String.format("Fail to refresh job caches, updateTime=%d, size=%d, IDs=%s",
                        updateTime, workflowIds.size(), getWorkflowIdsStr(workflowIds)), e);
            } finally {
                MultiTenantContext.setTenant(prevTenant);
            }
        }

        private void logUpdatedIds(List<Long> filteredIds, List<Long> originalIds) {
            Set<Long> idSet = new HashSet<>(filteredIds);
            List<Long> updatedIds = originalIds.stream().filter(id -> !idSet.contains(id)).collect(Collectors.toList());
            if (!updatedIds.isEmpty()) {
                log.info("Skipping already updated job caches, updateTime={}, size={}, IDs={}",
                        updateTime, updatedIds.size(), getWorkflowIdsStr(updatedIds));
            }
        }

        private List<Long> removeUpdatedIds(List<Long> ids, List<Long> times, @NotNull Long updateTime) {
            return IntStream.range(0, ids.size())
                    .mapToObj(idx -> Pair.of(ids.get(idx), times.get(idx)))
                    .filter(pair -> updateTime.equals(pair.getValue()))
                    .map(Pair::getKey)
                    .collect(Collectors.toList());
        }
    }
}
