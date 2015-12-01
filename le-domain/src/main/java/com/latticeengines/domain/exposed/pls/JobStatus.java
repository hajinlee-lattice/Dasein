package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonValue;

public enum JobStatus {

    // New status has to be at the end
    PENDING(0, "Pending"), //
    RUNNING(1, "Running"), //
    COMPLETED(2, "Completed"), //
    FAILED(3, "Failed"), //
    CANCELLED(4, "Cancelled");

    private JobStatus(int statusId, String status) {
        this.statusId = statusId;
        this.statusCode = status;
    }

    private int statusId;
    private String statusCode;

    public int getStatusId() {
        return statusId;
    }

    public String getStatusCode() {
        return statusCode;
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
