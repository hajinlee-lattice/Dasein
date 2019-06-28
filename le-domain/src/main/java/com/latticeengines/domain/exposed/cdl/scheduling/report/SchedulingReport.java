package com.latticeengines.domain.exposed.cdl.scheduling.report;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

/**
 * Aggregated scheduling report in some time period
 */
public class SchedulingReport {
    private final int startCycleId;

    private final int endCycleId;

    // scheduling cycle reports
    private final List<CycleReport> cycleReports;

    // tenantId => number of scheduled PAs
    private final Map<String, Integer> numScheduledPAs;

    public SchedulingReport(int startCycleId, int endCycleId, List<CycleReport> cycleReports,
            Map<String, Integer> numScheduledPAs) {
        Preconditions.checkArgument(startCycleId <= endCycleId,
                "Start cycle ID should not be larger than end cycle ID");
        this.startCycleId = startCycleId;
        this.endCycleId = endCycleId;
        this.cycleReports = cycleReports != null ? cycleReports : Collections.emptyList();
        this.numScheduledPAs = numScheduledPAs != null ? numScheduledPAs : Collections.emptyMap();
    }

    public int getStartCycleId() {
        return startCycleId;
    }

    public int getEndCycleId() {
        return endCycleId;
    }

    public List<CycleReport> getCycleReports() {
        return cycleReports;
    }

    public Map<String, Integer> getNumScheduledPAs() {
        return numScheduledPAs;
    }

    @Override
    public String toString() {
        return "SchedulingReport{" + "startCycleId=" + startCycleId + ", endCycleId=" + endCycleId + ", cycleReports="
                + cycleReports + ", numScheduledPAs=" + numScheduledPAs + '}';
    }
}
