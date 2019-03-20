package com.latticeengines.workflow.exposed.service;

import java.util.List;
import java.util.concurrent.Future;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public interface JobCacheService {
    /**
     * Retrieve a list of {@link WorkflowJob} with specified {@link Tenant} and transform them into {@link Job}. Have the
     * same read through cache behavior as {@link JobCacheService#getByWorkflowId(Long, boolean)} for individual object.
     *
     * @param tenant target tenant, should not be {@literal null} and has non-null {@link Tenant#getPid()}
     * @param includeDetails flag to include job details in the returned object
     * @return list of transformed {@link Job} (no {@literal null} in the list)
     */
    List<Job> getByTenant(@NotNull Tenant tenant, boolean includeDetails);

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

    /**
     * Delete job id list cache entry associated with the target {@link Tenant}. The input {@link Tenant} must not be
     * {@literal null} and has non-null {@link Tenant#getPid()}, otherwise this function will have no effect (noop)
     * @param tenant target tenant
     */
    void evict(Tenant tenant);

    /**
     * Delete job cache entries associated with the given list of workflow IDs. Noop if the list is {@literal null}.
     * Also, any {@literal null} workflow ID in the list will be skipped.
     * @param workflowIds target list of workflow IDs
     */
    void evictByWorkflowIds(@NotNull List<Long> workflowIds);

    /**
     * Delete all job cache entries belong to specified tenant. This operation can
     * be slow if there are a lot of jobs in the tenant. The input {@link Tenant}
     * must not be {@literal null} and has non-null {@link Tenant#getPid()},
     * otherwise this function will have no effect (noop)
     *
     * @param tenant
     *            target tenant
     * @return number of job cache entries evicted
     */
    int deepEvict(Tenant tenant);

    /**
     * Delete ALL job cache entries. This operation can be slow if there are a lot
     * of jobs. Use this function carefully.
     *
     * @return number of cache entries evicted
     */
    int evictAll();
}
