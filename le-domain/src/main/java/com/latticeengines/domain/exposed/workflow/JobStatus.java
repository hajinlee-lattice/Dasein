package com.latticeengines.domain.exposed.workflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;

import com.fasterxml.jackson.annotation.JsonValue;
import com.latticeengines.common.exposed.util.YarnUtils;

public enum JobStatus {

    // New status has to be at the end
    PENDING(0, "Pending", false), //
    RUNNING(1, "Running", false), //
    COMPLETED(2, "Completed", true), //
    FAILED(3, "Failed", true), //
    CANCELLED(4, "Cancelled", true), //
    SKIPPED(5, "Skipped", true), //
    READY(6, "Ready", false); //

    JobStatus(int statusId, String status, boolean terminated) {
        this.statusId = statusId;
        this.statusCode = status;
        this.terminated = terminated;
    }

    private int statusId;
    private String statusCode;
    private boolean terminated;
    private static Logger log = LoggerFactory.getLogger(JobStatus.class);

    public int getStatusId() {
        return statusId;
    }

    public String getStatusCode() {
        return statusCode;
    }

    public boolean isTerminated() {
        return terminated;
    }

    public void setTerminated(boolean terminated) {
        this.terminated = terminated;
    }

    private static Map<String, JobStatus> statusCodeMap = new HashMap<>();

    static {
        for (JobStatus summaryStatus : values()) {
            statusCodeMap.put(summaryStatus.getStatusCode(), summaryStatus);
        }
    }

    private static Map<JobStatus, List<String>> workflowJobStatusMap = null;

    private static void initWorkflowJobStatusMao() {
        workflowJobStatusMap = new HashMap<>();
        for (JobStatus jobStatus : values()) {
            List<String> mappedValues = new ArrayList<String>();
            System.out.println("*******************" + jobStatus);
            switch (jobStatus) {
            case PENDING:
                mappedValues.add(FinalApplicationStatus.UNDEFINED.name());
                mappedValues.add("PENDING"); //Noticed that some test cases are adding Status "PENDING" into workflowJob
                mappedValues.add("");
                break;
            case RUNNING:
                mappedValues.add(BatchStatus.STARTING.name());
                mappedValues.add(BatchStatus.STARTED.name());
                mappedValues.add(BatchStatus.STOPPING.name());
                break;
            case COMPLETED:
                mappedValues.add(FinalApplicationStatus.SUCCEEDED.name());
                mappedValues.add(BatchStatus.COMPLETED.name());
                break;
            case FAILED:
                mappedValues.add(FinalApplicationStatus.FAILED.name());
                mappedValues.add(FinalApplicationStatus.KILLED.name());
                mappedValues.add(BatchStatus.ABANDONED.name());
                mappedValues.add(BatchStatus.FAILED.name());
                mappedValues.add(BatchStatus.UNKNOWN.name());
                break;
            case CANCELLED:
                mappedValues.add(BatchStatus.STOPPED.name());
                break;
            default:
                mappedValues.add("");
            }
            workflowJobStatusMap.put(jobStatus, mappedValues);
        }
    }

    @JsonValue
    public String getName() {
        return StringUtils.capitalize(super.name().toLowerCase());
    }

    public static JobStatus getByStatusCode(String statusCode) {
        return statusCodeMap.get(statusCode);
    }

    public static JobStatus fromString(String status) {
        return fromString(status, null);
    }

    public static JobStatus fromString(String status, YarnApplicationState jobState) {
        status = status.toUpperCase();
        if (EnumUtils.isValidEnum(FinalApplicationStatus.class, status)) {
            FinalApplicationStatus jobStatus = FinalApplicationStatus.valueOf(status);
            return fromYarnStatus(jobStatus, jobState);
        } else if (EnumUtils.isValidEnum(BatchStatus.class, status)) {
            return fromBatchStatus(BatchStatus.valueOf(status));
        } else if (EnumUtils.isValidEnum(JobStatus.class, status)) {
            return JobStatus.valueOf(status);
        } else {
            log.warn("Got job status that cannot be handled. Status = " + status);
            return null;
        }
    }

    public static List<String> mappedWorkflowJobStatuses(String jobStatusStr) {
        if (workflowJobStatusMap == null) {
            initWorkflowJobStatusMao();
        }
        if (jobStatusStr == null) {
            return null;
        }
        if (EnumUtils.isValidEnum(JobStatus.class, jobStatusStr.toUpperCase())) {
            JobStatus jobStatus = JobStatus.valueOf(jobStatusStr.toUpperCase());
            return workflowJobStatusMap.get(jobStatus);
        }

        return Collections.singletonList(jobStatusStr);
    }

    public static JobStatus fromYarnStatus(FinalApplicationStatus status, YarnApplicationState jobState) {
        if (jobState != null) {
            if (jobState == YarnApplicationState.RUNNING) {
                return JobStatus.RUNNING;
            }
            if (jobState == YarnApplicationState.ACCEPTED) {
                return JobStatus.PENDING;
            }

            if (YarnUtils.FAILED_STATUS.contains(status)) {
                return JobStatus.FAILED;
            } else if (status == FinalApplicationStatus.UNDEFINED) {
                return JobStatus.RUNNING;
            } else {
                return null;
            }
        } else {
            switch (status) {
            case SUCCEEDED:
                return JobStatus.COMPLETED;
            case FAILED:
            case KILLED:
                return JobStatus.FAILED;
            case UNDEFINED:
            default:
                return JobStatus.PENDING;
            }
        }
    }

    private static JobStatus fromBatchStatus(BatchStatus status) {
        switch (status) {
        case COMPLETED:
            return JobStatus.COMPLETED;
        case STARTING:
        case STARTED:
        case STOPPING:
            return JobStatus.RUNNING;
        case STOPPED:
            return JobStatus.CANCELLED;
        case ABANDONED:
        case FAILED:
        case UNKNOWN:
            return JobStatus.FAILED;
        default:
            return JobStatus.PENDING;
        }
    }

}
