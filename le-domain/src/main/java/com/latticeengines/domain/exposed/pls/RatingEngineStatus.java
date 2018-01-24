package com.latticeengines.domain.exposed.pls;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum RatingEngineStatus {

    ACTIVE(0), //
    INACTIVE(1), //
    DELETED(2);

    private int stautsId;

    private static Map<Integer, RatingEngineStatus> statusMap = new HashMap<>();
    private static Map<RatingEngineStatus, Collection<RatingEngineStatus>> transitionMap = new HashMap<>();

    static {
        for (RatingEngineStatus status : values()) {
            statusMap.put(status.getStatusId(), status);
        }
        transitionMap.put(ACTIVE, Stream.of(INACTIVE).collect(Collectors.toSet()));
        transitionMap.put(INACTIVE, Stream.of(ACTIVE, DELETED).collect(Collectors.toSet()));
        transitionMap.put(DELETED, Stream.of(INACTIVE).collect(Collectors.toSet()));
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

    public static boolean canTransit(RatingEngineStatus srcState, RatingEngineStatus dstState) {
        if (transitionMap.containsKey(srcState) && transitionMap.get(srcState).contains(dstState)) {
            return true;
        }
        return false;
    }

}
