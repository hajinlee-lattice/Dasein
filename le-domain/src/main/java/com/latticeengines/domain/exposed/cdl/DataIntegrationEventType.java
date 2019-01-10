package com.latticeengines.domain.exposed.cdl;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum DataIntegrationEventType {
    WORKFLOW_SUBMITTED, WORKFLOW_STARTED, WORKFLOW_COMPLETED, WORKFLOW_FAILED;

    private static Map<DataIntegrationEventType, Collection<DataIntegrationEventType>> transitionMap = new HashMap<>();

    static {
        Set<DataIntegrationEventType> statesAfterSubmitted = new HashSet<>();
        statesAfterSubmitted.add(WORKFLOW_STARTED);
        statesAfterSubmitted.add(WORKFLOW_COMPLETED);
        statesAfterSubmitted.add(WORKFLOW_FAILED);
        transitionMap.put(WORKFLOW_SUBMITTED, statesAfterSubmitted);

        Set<DataIntegrationEventType> statesAfterStarted = new HashSet<>();
        statesAfterStarted.add(WORKFLOW_COMPLETED);
        statesAfterStarted.add(WORKFLOW_FAILED);
        transitionMap.put(WORKFLOW_STARTED, statesAfterStarted);
    }

    public static boolean canTransit(DataIntegrationEventType srcState,
            DataIntegrationEventType dstState) {
        if (transitionMap.containsKey(srcState) && transitionMap.get(srcState).contains(dstState)) {
            return true;
        }
        return false;
    }
}
