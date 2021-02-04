package com.latticeengines.domain.exposed.cdl;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum DataIntegrationEventType {
    WorkflowSubmitted, ExportStart, Initiated, InProgress, Completed, Failed, AudienceCreation, //
    AudienceSizeUpdate, DestinationAccountCreation, AuthInvalidated;

    private static Map<DataIntegrationEventType, Collection<DataIntegrationEventType>> transitionMap = new HashMap<>();

    static {
        Set<DataIntegrationEventType> statesAfterSubmitted = new HashSet<>();
        statesAfterSubmitted.add(ExportStart);
        statesAfterSubmitted.add(Initiated);
        statesAfterSubmitted.add(InProgress);
        statesAfterSubmitted.add(Failed);
        statesAfterSubmitted.add(Completed);
        statesAfterSubmitted.add(AudienceCreation);
        statesAfterSubmitted.add(AudienceSizeUpdate);
        transitionMap.put(WorkflowSubmitted, statesAfterSubmitted);

        Set<DataIntegrationEventType> statesAfterExportStarted = new HashSet<>();
        statesAfterExportStarted.add(Initiated);
        statesAfterExportStarted.add(InProgress);
        statesAfterExportStarted.add(Failed);
        statesAfterExportStarted.add(Completed);
        statesAfterExportStarted.add(AudienceCreation);
        statesAfterExportStarted.add(AudienceSizeUpdate);
        transitionMap.put(ExportStart, statesAfterExportStarted);

        Set<DataIntegrationEventType> statesAfterInitiated = new HashSet<>();
        statesAfterInitiated.add(InProgress);
        statesAfterInitiated.add(Failed);
        statesAfterInitiated.add(Completed);
        statesAfterInitiated.add(AudienceCreation);
        statesAfterInitiated.add(AudienceSizeUpdate);
        transitionMap.put(Initiated, statesAfterInitiated);

        Set<DataIntegrationEventType> statesAfterAudienceCreation = new HashSet<>();
        statesAfterAudienceCreation.add(InProgress);
        statesAfterAudienceCreation.add(Failed);
        statesAfterAudienceCreation.add(Completed);
        statesAfterAudienceCreation.add(AudienceSizeUpdate);
        transitionMap.put(AudienceCreation, statesAfterAudienceCreation);

        Set<DataIntegrationEventType> statesAfterInProgress = new HashSet<>();
        statesAfterInProgress.add(Failed);
        statesAfterInProgress.add(Completed);
        statesAfterInProgress.add(AudienceSizeUpdate);
        transitionMap.put(InProgress, statesAfterInProgress);

        Set<DataIntegrationEventType> statesAfterCompleted = new HashSet<>();
        statesAfterCompleted.add(AudienceSizeUpdate);
        transitionMap.put(Completed, statesAfterCompleted);

        Set<DataIntegrationEventType> statesAfterFailed = new HashSet<>();
        statesAfterFailed.add(AudienceSizeUpdate);
        transitionMap.put(Failed, statesAfterFailed);
    }

    public static boolean canTransit(DataIntegrationEventType srcState,
            DataIntegrationEventType dstState) {
        if (transitionMap.containsKey(srcState) && transitionMap.get(srcState).contains(dstState)) {
            return true;
        }
        return false;
    }
}
