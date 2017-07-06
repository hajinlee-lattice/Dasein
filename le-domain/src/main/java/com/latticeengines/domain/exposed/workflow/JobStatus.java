package com.latticeengines.domain.exposed.workflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonValue;

public enum JobStatus {

    // New status has to be at the end
    PENDING(0, "Pending", false), //
    RUNNING(1, "Running", false), //
    COMPLETED(2, "Completed", true), //
    FAILED(3, "Failed", true), //
    CANCELLED(4, "Cancelled", true), //
    SKIPPED(5, "Skipped", true); //

    private JobStatus(int statusId, String status, boolean terminated) {
        this.statusId = statusId;
        this.statusCode = status;
        this.terminated = terminated;
    }

    private int statusId;
    private String statusCode;
    private boolean terminated;

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

    @JsonValue
    public String getName() {
        return StringUtils.capitalize(super.name().toLowerCase());
    }

    public static JobStatus getByStatusCode(String statusCode) {
        return statusCodeMap.get(statusCode);
    }
}
