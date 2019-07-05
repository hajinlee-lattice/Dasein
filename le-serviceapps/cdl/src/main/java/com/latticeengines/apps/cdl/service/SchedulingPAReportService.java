package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.scheduling.report.CycleReport;
import com.latticeengines.domain.exposed.cdl.scheduling.report.PAJobReport;
import com.latticeengines.domain.exposed.cdl.scheduling.report.SchedulingReport;
import com.latticeengines.domain.exposed.cdl.scheduling.report.TenantReport;

/**
 * Service to save/retrieve PA scheduling results
 */
public interface SchedulingPAReportService {

    /**
     * Save scheduling result of one cycle. {@link CycleReport#getCycleId()} will be
     * automatically generated in this method
     *
     * @param cycleReport
     *            scheduling result of this cycle
     * @param decisions
     *            scheduling decision for multiple tenants
     * @return generated cycle ID
     */
    int saveSchedulingResult(CycleReport cycleReport, Map<String, List<TenantReport.SchedulingDecision>> decisions);

    /**
     * Cleanup all scheduling results with cycle ID less than or equal to given ID. This operation can be heavy.
     *
     * @param lastCycleId
     *            max cycle ID that will be deleted
     * @return number of cycles cleaned up
     */
    int cleanupOldReports(int lastCycleId);

    /**
     * Retrieve cycle ID range that is currently tracked
     * 
     * @return pair of start cycle ID (inclusive), end cycle ID (exclusive)
     */
    Pair<Integer, Integer> getCurrentCycleIdRange();

    /**
     * Retrieve overall scheduling report
     *
     * @return report object
     */
    SchedulingReport getSchedulingReport();

    /**
     * Retrieve scheduling report with info within given cycle ID range.
     *
     * @param startCycleId
     *            start cycle ID, inclusive
     * @param endCycleId
     *            end cycle ID, exclusive
     * @return report object
     */
    SchedulingReport getSchedulingReport(int startCycleId, int endCycleId);

    /**
     * Retrieve scheduling report with info within given time period.
     *
     * @param startScheduleTime
     *            start schedule timestamp, inclusive
     * @param endScheduleTime
     *            end schedule timestamp, exclusive
     * @return report object
     */
    SchedulingReport getSchedulingReport(long startScheduleTime, long endScheduleTime);

    /**
     * Retrieve scheduling report of given cycle
     *
     * @param cycleId
     *            target cycle ID
     * @return report object, {@literal null} if no report found
     */
    CycleReport getCycleReport(int cycleId);

    /**
     * Retrieve scheduling report for given tenant
     *
     * @param tenantId
     *            target tenant ID
     * @return report object, {@literal null} if tenant does not exist
     */
    TenantReport getTenantReport(@NotNull String tenantId);

    /**
     * Retrieve scheduling report for given tenant within given cycle ID range
     *
     * @param tenantId
     *            target tenant ID
     * @param startCycleId
     *            start cycle ID, inclusive
     * @param endCycleId
     *            end cycle ID, exclusive
     * @return report object, {@literal null} if tenant does not exist
     */
    TenantReport getTenantReport(@NotNull String tenantId, int startCycleId, int endCycleId);

    /**
     * Retrieve scheduling report for given tenant within given time period
     *
     * @param tenantId
     *            target tenant ID
     * @param startScheduleTime
     *            start schedule timestamp, inclusive
     * @param endScheduleTime
     *            end schedule timestamp, exclusive
     * @return report object, {@literal null} if tenant does not exist
     */
    TenantReport getTenantReport(@NotNull String tenantId, long startScheduleTime, long endScheduleTime);

    /**
     * Retrieve scheduling report for given PA job
     *
     * @param applicationId
     *            job application ID
     * @return report object, {@literal null} if no such job exist
     */
    PAJobReport getPAJobReport(@NotNull String applicationId);
}
