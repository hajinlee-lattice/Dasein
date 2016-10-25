package com.latticeengines.domain.exposed.datacloud.match;

import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Traveler {

    private static final Log log = LogFactory.getLog(Traveler.class);

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

    private void logVisit(String microEngine) {
        log.info(String.format("Traveler %s visited micro engine %s. RootOperationUID=%s", getTravelerId(), microEngine,
                getRootOperationUid()));
    }

}
