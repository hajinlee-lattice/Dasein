package com.latticeengines.workflow.service.impl;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.service.ReportService;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobCache;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.workflow.cache.JobCacheWriter;
import com.latticeengines.workflow.core.LEJobExecutionRetriever;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.JobCacheService;
import com.latticeengines.workflow.exposed.util.WorkflowJobUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Service("jobCacheService")
public class JobCacheServiceImpl implements JobCacheService {

    private static final int NUM_THREADS = 8;
    private static final int EXPIRE_TIME_IN_MILLIS = 30000;

    @Value("${hadoop.yarn.timeline-service.webapp.address}")
    private String timelineServiceUrl;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private LEJobExecutionRetriever leJobExecutionRetriever;

    @Autowired
    private ReportService reportService;

    @Autowired
    private JobCacheWriter cacheWriter;

    private ExecutorService service = ThreadPoolUtils.getFixedSizeThreadPool("job-cache-service", NUM_THREADS);

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

        Map<Long, JobCache> cacheJobMap = cacheWriter
                .getByWorkflowIds(workflowIds, includeDetails)
                .stream()
                // consider a cache miss if the cache object is not valid
                .filter(this::isCacheValid)
                .collect(Collectors.toMap(cache -> cache.getJob().getId(), cache -> cache));
        // trigger async refresh for expired cache, still return stale data
        List<Long> expiredWorkflowIds = cacheJobMap.values()
                .stream()
                .filter(this::isCacheExpired)
                .map(cache -> cache.getJob().getId())
                .collect(Collectors.toList());
        putAsync(expiredWorkflowIds, false);

        // load jobs from datastore
        List<Long> missingJobIds = workflowIds
                .stream()
                .filter(id -> !cacheJobMap.containsKey(id))
                .collect(Collectors.toList());
        List<Job> missingJobs = workflowJobEntityMgr.findByWorkflowIds(missingJobIds)
                .stream()
                .map(workflowJob -> transform(workflowJob, includeDetails))
                .collect(Collectors.toList());

        // populate for cache misses
        cacheWriter.put(missingJobs, includeDetails);

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

    /*
     * if forceRefresh is
     * true: cache entry will be populated
     * false: only populate the cache entry if no one else is updating
     */
    private Future<?> putAsync(List<Long> workflowIds, boolean forceRefresh) {
        long timestamp = System.currentTimeMillis();
        if (!forceRefresh) {
            // when not forcing refresh, set the cache entry's update time
            // runnable only update if no other people's updating
            cacheWriter.setUpdateTimeByWorkflowIds(workflowIds, true, timestamp);
            cacheWriter.setUpdateTimeByWorkflowIds(workflowIds, false, timestamp);
        }
        return service.submit(new RefreshCacheRunnable(workflowIds, forceRefresh ? null : timestamp));
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
        return timestamp - cache.getUpdatedAt() > EXPIRE_TIME_IN_MILLIS;
    }

    /*
     * refresh the cache entry asynchronously if it expired and return the job in the current entry
     * (may be stale)
     */
    private Job handleJobCache(JobCache jobCache, long workflowId) {
        // cache hit
        if (isCacheExpired(jobCache)) {
            // trigger cache update asynchronously and return stale data
            putAsync(Collections.singletonList(workflowId), false);
        }
        return jobCache.getJob();
    }

    private void check(WorkflowJob workflowJob) {
        Preconditions.checkNotNull(workflowJob);
        Preconditions.checkNotNull(workflowJob.getWorkflowId());
    }

    private boolean isCacheValid(JobCache cache) {
        return cache != null && cache.getJob() != null && cache.getJob().getId() != null;
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
        return WorkflowJobUtils.assembleJob(reportService, leJobExecutionRetriever, timelineServiceUrl,
                workflowJob, includeDeatils);
    }

    /*
     * Asynchronously refresh cache entries for target jobs.
     */
    private class RefreshCacheRunnable implements Runnable {
        private List<Long> workflowIds;
        private Long updateTime;

        RefreshCacheRunnable(@NotNull List<Long> workflowIds, @Nullable Long updateTime) {
            Preconditions.checkNotNull(workflowIds);
            this.workflowIds = workflowIds.stream().filter(Objects::nonNull).collect(Collectors.toList());
            this.updateTime = updateTime;
        }

        @Override
        public void run() {
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

            // merge IDs and dedup
            List<Long> jobIds = Stream.concat(ids.stream(), detailIds.stream()).distinct().collect(Collectors.toList());
            List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByWorkflowIds(jobIds);
            Set<Long> idSet = new HashSet<>(ids);
            Set<Long> detailIdSet = new HashSet<>(detailIds);

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
            cacheWriter.put(jobs, false);
            cacheWriter.put(detailJobs, true);
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
