package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

public enum PredictorStatus {

    NOT_USED_FOR_BUYER_INSIGHTS("0", false), //
    USED_FOR_BUYER_INSIGHTS("1", true); //

    private String statusCode;
    private boolean status;

    private PredictorStatus(String statusCode, boolean status) {
        this.statusCode = statusCode;
        this.status = status;
    }

    public String getStatusCode() {
        return this.statusCode;
    }

    public boolean getStatus() {
        return this.status;
    }

    private static Map<String, PredictorStatus> statusMap = new HashMap<String, PredictorStatus>();

    static {
        for (PredictorStatus predictorStatus : PredictorStatus.values()) {
            statusMap.put(predictorStatus.getStatusCode(), predictorStatus);
        }
    }

    public static PredictorStatus getStatusByName(String statusName) {
        return statusMap.get(statusName);
    }

    public static String getflippedStatusCode(boolean currentStatus) {
        PredictorStatus[] vals = values();
        for (String statusCode : statusMap.keySet()) {
            if (statusMap.get(statusCode).getStatus() == currentStatus) {
                PredictorStatus status = statusMap.get(statusCode);
                return vals[(status.ordinal() + 1) % vals.length].getStatusCode();
            }
        }
        return "";
    }
}
