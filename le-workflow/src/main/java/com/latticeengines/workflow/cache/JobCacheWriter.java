package com.latticeengines.workflow.cache;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobCache;

public interface JobCacheWriter {
    /**
     * Retrieve {@link JobCache} by the workflow job's ID.
     *
     * @param workflowId     job's ID, cannot be {@literal null}
     * @param includeDetails flag to indicate whether job detail should be included in the retrieved object
     * @return {@link JobCache} object, {@literal null} if the cache entry does not exist
     */
    JobCache getByWorkflowId(@NotNull Long workflowId, boolean includeDetails);

    /**
     * Retrieve a list of {@link JobCache} with specified workflow job IDs.
     *
     * @param workflowIds    a list of target job ID, the list and each item cannot be {@literal null}
     * @param includeDetails flag to indicate whether job detail should be included in the retrieved objects
     * @return a list of {@link JobCache} objects with the same length as the list of IDs,
     * if a specific cache entry does not exist, {@literal null} will be inserted in the respective location.
     */
    List<JobCache> getByWorkflowIds(@NotNull List<Long> workflowIds, boolean includeDetails);

    /**
     * Retrieve update times (in millisecond) of specified {@link JobCache} entries.
     *
     * @param workflowIds    a list of target job ID, the list and each item cannot be {@literal null}
     * @param includeDetails used to identify the cache entry
     * @return a list of timestamps, if a specific cache entry does not exist,
     * {@literal null} will be inserted in the respective location.
     */
    List<Long> getUpdateTimeByWorkflowIds(@NotNull List<Long> workflowIds, boolean includeDetails);

    /**
     * Set the update times (in millisecond) of specified {@link JobCache} entries.
     *
     * @param workflowIds    a list of target job ID, the list and each item cannot be {@literal null}
     * @param includeDetails used to identify the cache entry
     * @param timestamp      timestamp to be set
     */
    void setUpdateTimeByWorkflowIds(List<Long> workflowIds, boolean includeDetails, long timestamp);

    /**
     * Populate {@link JobCache} cache entries for the specified {@link Job}.
     *
     * @param job            target job object
     * @param includeDetails used to identify the cache entry
     */
    void put(Job job, boolean includeDetails);

    /**
     * Populate {@link JobCache} cache entries for specified {@link Job}s.
     *
     * @param jobs           a list of target job objects
     * @param includeDetails used to identify the cache entry
     */
    void put(List<Job> jobs, boolean includeDetails);

    /**
     * Clear cache entries for specified job
     *
     * @param job            target job
     * @param includeDetails used to identify the cache entry
     */
    void clear(Job job, boolean includeDetails);

    /**
     * Clear all job cache entries. This operation can be slow if there are a lot of
     * entries. Be caution when using this.
     *
     * @return number of cache entries deleted
     */
    int clearAll();
}
