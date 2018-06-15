package com.latticeengines.domain.exposed.pls;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Sets;

public enum RatingEngineStatus {

    ACTIVE(0), //
    INACTIVE(1); //

    private int stautsId;

    private static Map<Integer, RatingEngineStatus> statusMap = new HashMap<>();
    private static Map<RatingEngineStatus, Collection<RatingEngineStatus>> transitionMap = new HashMap<>();

    static {
        for (RatingEngineStatus status : values()) {
            statusMap.put(status.getStatusId(), status);
        }
        transitionMap.put(ACTIVE, Sets.newHashSet(INACTIVE));
        transitionMap.put(INACTIVE, Sets.newHashSet(ACTIVE));
    }

    RatingEngineStatus(int statusId) {
        this.stautsId = statusId;
    }

    public int getStatusId() {
        return this.stautsId;
    }

    public RatingEngineStatus getRatingEngineStatus(Integer statusId) {
        return statusMap.get(statusId);
    }

    public boolean canTransition(RatingEngineStatus transitionState) {
        return this.equals(transitionState)
                || transitionMap.containsKey(this) && transitionMap.get(this).contains(transitionState);
    }

}
