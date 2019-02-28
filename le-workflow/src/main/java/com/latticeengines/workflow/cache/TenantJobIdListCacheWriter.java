package com.latticeengines.workflow.cache;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobListCache;

public interface TenantJobIdListCacheWriter {
    /**
     *
     * Retrieve all job IDs associated with the target tenant from cache, the returned {@link Job} object will contain
     * at least one of the following fields {@link Job#getId()}, {@link Job#getPid()} or {@link Job#getApplicationId()}
     * @param tenant target tenant
     * @return cached job list, {@literal null} if no cache entry exists
     */
    JobListCache get(@NotNull Tenant tenant);

    /**
     *
     * Associate specified job IDs to the given tenant in cache, each input {@link Job} object will contain
     * at least one of the following fields {@link Job#getId()}, {@link Job#getPid()} or {@link Job#getApplicationId()}
     * @param tenant target tenant
     * @param jobs list of specified jobs
     * @return
     */
    void set(@NotNull Tenant tenant, @NotNull List<Job> jobs);

    /**
     *
     * Clear the job IDs associated with the given tenant in cache
     * @param tenant target tenant
     */
    void clear(@NotNull Tenant tenant);
}
