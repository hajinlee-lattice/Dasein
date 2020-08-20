package com.latticeengines.apps.cdl.service;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.scheduling.PAQuotaSummary;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

/**
 * Service to handle different quota for PA
 */
public interface PAQuotaService {

    /**
     * Given current system/tenant state, check whether tenant still have specific
     * PA quota
     *
     * @param tenantId
     *            target tenant
     * @param quotaName
     *            quota to check
     * @param completedPAJobs
     *            recently completed {@link ProcessAnalyzeWorkflowConfiguration} job
     * @param now
     *            current time, use current system time if not provided
     * @param timezone
     *            configured timezone for this tenant, use <code>UTC</code> if not
     *            provided
     * @return flag indicate whether tenant still have quota remaining or not
     */
    boolean hasQuota(@NotNull String tenantId, @NotNull String quotaName, List<WorkflowJob> completedPAJobs,
            Instant now, ZoneId timezone);

    /**
     * Retrieve configured PA quota for specific tenant
     *
     * @param tenantId
     *            target tenant
     * @return map of quota name -> no. allowed PA
     */
    Map<String, Long> getTenantPaQuota(@NotNull String tenantId);

    /**
     * Merge (replace based on quota name) input pa quota into existing one
     *
     * @param tenantId
     *            target tenant
     * @param paQuota
     *            quota name -> quota value map
     * @return merged pa quota for target tenant
     */
    Map<String, Long> setTenantPaQuota(@NotNull String tenantId, @NotNull Map<String, Long> paQuota);

    /**
     * Retrieve/calculate PA quota related info for specific tenant, including
     * remaining quota, quota reset time and others.
     *
     * @param tenantId
     *            target tenant
     * @param completedPAJobs
     *            recently completed {@link ProcessAnalyzeWorkflowConfiguration} job
     * @param now
     *            current time, use current system time if not provided
     * @param timezone
     *            configured timezone for this tenant, use <code>UTC</code> if not
     *            provided
     * @return summary of specified tenant
     */
    PAQuotaSummary getPAQuotaSummary(@NotNull String tenantId, List<WorkflowJob> completedPAJobs, Instant now,
            ZoneId timezone);
}
