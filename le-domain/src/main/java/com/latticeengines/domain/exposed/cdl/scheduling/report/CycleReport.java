package com.latticeengines.domain.exposed.cdl.scheduling.report;

import java.util.Collections;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

/**
 * Info about a single scheduling cycle
 */
public class CycleReport {
    private final int cycleId;

    private final long scheduledTime;

    private final Summary summary;

    public CycleReport(int cycleId, long scheduledTime, @NotNull Summary summary) {
        Preconditions.checkNotNull(summary, "Scheduling cycle summary should not be null");
        this.cycleId = cycleId;
        this.scheduledTime = scheduledTime;
        this.summary = summary;
    }

    public int getCycleId() {
        return cycleId;
    }

    public long getScheduledTime() {
        return scheduledTime;
    }

    public Summary getSummary() {
        return summary;
    }

    @Override
    public String toString() {
        return "CycleReport{" + "cycleId=" + cycleId + ", scheduledTime=" + scheduledTime + ", summary=" + summary
                + '}';
    }

    /**
     * Summary and stats
     */
    public static class Summary {
        private int numPA;

        private int numLargePA;

        private int numScheduleNowPA;

        /*
         * tenantId => applicationId that are scheduled in this cycle
         */
        private Map<String, String> scheduledNewPAs;
        private Map<String, String> scheduledRetryPAs;

        public Summary(int numPA, int numLargePA, int numScheduleNowPA, Map<String, String> scheduledNewPAs,
                Map<String, String> scheduledRetryPAs) {
            this.numPA = numPA;
            this.numLargePA = numLargePA;
            this.numScheduleNowPA = numScheduleNowPA;
            this.scheduledNewPAs = scheduledNewPAs != null ? scheduledNewPAs : Collections.emptyMap();
            this.scheduledRetryPAs = scheduledRetryPAs != null ? scheduledRetryPAs : Collections.emptyMap();
        }

        public int getNumPA() {
            return numPA;
        }

        public int getNumLargePA() {
            return numLargePA;
        }

        public int getNumScheduleNowPA() {
            return numScheduleNowPA;
        }

        public Map<String, String> getScheduledNewPAs() {
            return scheduledNewPAs;
        }

        public Map<String, String> getScheduledRetryPAs() {
            return scheduledRetryPAs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Summary summary = (Summary) o;
            return numPA == summary.numPA && numLargePA == summary.numLargePA
                    && numScheduleNowPA == summary.numScheduleNowPA
                    && Objects.equal(scheduledNewPAs, summary.scheduledNewPAs)
                    && Objects.equal(scheduledRetryPAs, summary.scheduledRetryPAs);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(numPA, numLargePA, numScheduleNowPA, scheduledNewPAs, scheduledRetryPAs);
        }

        @Override
        public String toString() {
            return "Summary{" + "numPA=" + numPA + ", numLargePA=" + numLargePA + ", numScheduleNowPA="
                    + numScheduleNowPA + ", scheduledNewPAs=" + scheduledNewPAs + ", scheduledRetryPAs="
                    + scheduledRetryPAs + '}';
        }
    }
}
