package com.latticeengines.workflow.exposed.service;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

import java.util.List;
import java.util.concurrent.Future;

public interface JobCacheService {
    /**
     * Try to retrieve {@link WorkflowJob} with specified workflow ID from cache first. If the cache entry does not
     * exist, retrieve from datastore and populate the cache. Transform the retrieved object to {@link Job} and return
     * it afterwards. Return {@literal null} if the specified job does not exist.
     *
     * @param workflowId     workflow ID of the target job, should not be {@literal null}
     * @param includeDetails flag to include job details in the returned object
     * @return transformed {@link Job} or {@literal null} if the target job does not exist in datastore.
     */
    Job getByWorkflowId(Long workflowId, boolean includeDetails);

    /**
     * Retrieve a list of {@link WorkflowJob} with specified workflow IDs and transform them into {@link Job}. Have the
     * same read through cache behavior as {@link JobCacheService#getByWorkflowId(Long, boolean)} for individual object.
     *
     * @param workflowIds    list of workflow IDs of the target jobs, should not be {@literal null}
     * @param includeDetails flag to include job details in the returned object
     * @return list of transformed {@link Job} (no {@literal null} in the list)
     */
    List<Job> getByWorkflowIds(List<Long> workflowIds, boolean includeDetails);

    /**
     * Transform the given {@link WorkflowJob} into {@link Job} and populate the cache entry.
     *
     * @param workflowJob workflow job object used to populate cache, should not be {@literal null}
     */
    void put(WorkflowJob workflowJob);

    /**
     * Retrieve the {@link WorkflowJob} by its workflow ID and populate {@link Job} cache entry asynchronously.
     *
     * @param workflowId target workflow ID, should not be {@literal null}
     * @return a future to represent the cache refresh task
     */
    Future<?> putAsync(Long workflowId);

    /**
     * Retrieve {@link WorkflowJob} with a list of workflow IDs and populate {@link Job} cache entries asynchronously.
     *
     * @param workflowIds list of target workflow IDs, should not be {@literal null}
     * @return a future to represent the cache refresh task
     */
    Future<?> putAsync(List<Long> workflowIds);
}
