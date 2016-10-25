package com.latticeengines.domain.exposed.datacloud.match;

import java.util.UUID;

public class Traveler {

    private final String rootOperationUid;
    private final String travelerId;

    public Traveler(String rootOperationUid) {
        travelerId = UUID.randomUUID().toString();
        this.rootOperationUid = rootOperationUid;
    }

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public String getTravelerId() {
        return travelerId;
    }

}
