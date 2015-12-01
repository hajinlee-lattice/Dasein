package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonValue;

public enum StepStatus {

    // New status has to be at the end
    PENDING(0, "Pending"), //
    RUNNING(1, "Running"), //
    COMPLETED(2, "Completed"), //
    FAILED(3, "Failed"), //
    CANCELLED(4, "Cancelled");

    private StepStatus(int statusId, String status) {
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

    private static Map<String, StepStatus> statusCodeMap = new HashMap<>();

    static {
        for (StepStatus summaryStatus : values()) {
            statusCodeMap.put(summaryStatus.getStatusCode(), summaryStatus);
        }
    }

    @JsonValue
    public String getName() {
        return StringUtils.capitalize(super.name().toLowerCase());
    }

    public static StepStatus getByStatusCode(String statusCode) {
        return statusCodeMap.get(statusCode);
    }
}
