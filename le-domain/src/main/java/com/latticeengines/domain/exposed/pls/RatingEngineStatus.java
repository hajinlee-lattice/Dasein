package com.latticeengines.domain.exposed.pls;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Sets;

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
        transitionMap.put(ACTIVE, Sets.newHashSet(INACTIVE));
        transitionMap.put(INACTIVE, Sets.newHashSet(ACTIVE, DELETED));
        transitionMap.put(DELETED, Sets.newHashSet(INACTIVE));
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
        return srcState.equals(dstState) || transitionMap.containsKey(srcState) && transitionMap.get(srcState).contains(dstState);
    }

}
