package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

public enum RatingEngineStatus {

    ACTIVE(0), //
    INACTIVE(1), //
    DELETED(2);

    private int stautsId;

    private static Map<Integer, RatingEngineStatus> statusMap = new HashMap<>();

    static {
        for (RatingEngineStatus status : values()) {
            statusMap.put(status.getStatusId(), status);
        }
    }

    private RatingEngineStatus(int statusId) {
        this.stautsId = statusId;
    }

    public int getStatusId() {
        return this.stautsId;
    }

    public RatingEngineStatus getRatingEngineStatus(Integer statusId) {
        return statusMap.get(statusId);
    }
}
