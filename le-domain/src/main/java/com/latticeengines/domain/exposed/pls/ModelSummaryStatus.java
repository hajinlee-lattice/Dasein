package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ModelSummaryStatus {

    // New status has to be at the end
    ACTIVE(0, "UpdateAsActive"), //
    INACTIVE(1, "UpdateAsInactive"), //
    DELETED(2, "UpdateAsDeleted");

    private static Map<String, ModelSummaryStatus> statusCodeMap = new HashMap<>();

    static {
        for (ModelSummaryStatus summaryStatus : values()) {
            statusCodeMap.put(summaryStatus.getStatusCode(), summaryStatus);
        }
    }

    private int statusId;
    private String statusCode;

    ModelSummaryStatus(int statusId, String status) {
        this.statusId = statusId;
        this.statusCode = status;
    }

    public static ModelSummaryStatus getByStatusCode(String statusCode) {
        return statusCodeMap.get(statusCode);
    }

    public int getStatusId() {
        return statusId;
    }

    public String getStatusCode() {
        return statusCode;
    }

    @JsonValue
    public String getName() {
        return StringUtils.capitalize(super.name().toLowerCase());
    }
}
