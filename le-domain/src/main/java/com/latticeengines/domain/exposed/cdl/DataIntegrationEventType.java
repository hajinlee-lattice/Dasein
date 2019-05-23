package com.latticeengines.domain.exposed.cdl;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum DataIntegrationEventType {
    WorkflowSubmitted, ExportStart, Initiated, InProgress, Completed, Failed, Progress, AudienceCreation;

    private static Map<DataIntegrationEventType, Collection<DataIntegrationEventType>> transitionMap = new HashMap<>();

    static {
        Set<DataIntegrationEventType> statesAfterSubmitted = new HashSet<>();
        statesAfterSubmitted.add(ExportStart);
        statesAfterSubmitted.add(Initiated);
        statesAfterSubmitted.add(InProgress);
        statesAfterSubmitted.add(Failed);
        statesAfterSubmitted.add(Completed);
        statesAfterSubmitted.add(Progress);
        statesAfterSubmitted.add(AudienceCreation);
        transitionMap.put(WorkflowSubmitted, statesAfterSubmitted);

        Set<DataIntegrationEventType> statesAfterExportStarted = new HashSet<>();
        statesAfterExportStarted.add(Initiated);
        statesAfterExportStarted.add(InProgress);
        statesAfterExportStarted.add(Failed);
        statesAfterExportStarted.add(Completed);
        statesAfterExportStarted.add(Progress);
        statesAfterExportStarted.add(AudienceCreation);
        transitionMap.put(ExportStart, statesAfterExportStarted);

        Set<DataIntegrationEventType> statesAfterInitiated = new HashSet<>();
        statesAfterInitiated.add(InProgress);
        statesAfterInitiated.add(Failed);
        statesAfterInitiated.add(Completed);
        statesAfterInitiated.add(Progress);
        statesAfterInitiated.add(AudienceCreation);
        transitionMap.put(Initiated, statesAfterInitiated);

        Set<DataIntegrationEventType> statesAfterAudienceCreation = new HashSet<>();
        statesAfterAudienceCreation.add(InProgress);
        statesAfterAudienceCreation.add(Failed);
        statesAfterAudienceCreation.add(Completed);
        statesAfterAudienceCreation.add(Progress);
        transitionMap.put(AudienceCreation, statesAfterAudienceCreation);

        Set<DataIntegrationEventType> statesAfterInProgress = new HashSet<>();
        statesAfterInProgress.add(Failed);
        statesAfterInProgress.add(Completed);
        transitionMap.put(InProgress, statesAfterInProgress);

        Set<DataIntegrationEventType> statesAfterProgress = new HashSet<>();
        statesAfterProgress.add(Failed);
        statesAfterProgress.add(Completed);
        transitionMap.put(Progress, statesAfterProgress);
    }

    public static boolean canTransit(DataIntegrationEventType srcState,
            DataIntegrationEventType dstState) {
        if (transitionMap.containsKey(srcState) && transitionMap.get(srcState).contains(dstState)) {
            return true;
        }
        return false;
    }
}
