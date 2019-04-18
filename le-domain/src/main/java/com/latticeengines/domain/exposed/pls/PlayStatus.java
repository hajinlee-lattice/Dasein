package com.latticeengines.domain.exposed.pls;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum PlayStatus {

    ACTIVE(0), //
    INACTIVE(1);

    private static Map<Integer, PlayStatus> statusMap = new HashMap<>();
    private static Map<PlayStatus, Collection<PlayStatus>> transitionMap = new HashMap<>();

    static {
        for (PlayStatus status : values()) {
            statusMap.put(status.getStatusId(), status);
        }
        transitionMap.put(ACTIVE, Stream.of(INACTIVE).collect(Collectors.toSet()));
        transitionMap.put(INACTIVE, Stream.of(ACTIVE).collect(Collectors.toSet()));
    }

    private int stautsId;

    PlayStatus(int statusId) {
        this.stautsId = statusId;
    }

    public static boolean canTransit(PlayStatus srcState, PlayStatus dstState) {
        if (transitionMap.containsKey(srcState) && transitionMap.get(srcState).contains(dstState)) {
            return true;
        }
        return false;
    }

    public int getStatusId() {
        return this.stautsId;
    }

    public PlayStatus getPlayStatus(Integer statusId) {
        return statusMap.get(statusId);
    }
}
