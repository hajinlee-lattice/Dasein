package com.latticeengines.domain.exposed.pls;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum ActionType {

    CDL_DATAFEED_IMPORT_WORKFLOW("cdlDataFeedImportWorkflow", "Import"), //
    CDL_OPERATION_WORKFLOW("cdlOperationWorkflow", "Delete"), //
    RATING_ENGINE_CHANGE("ratingEngineChange", "Scoring"), //
    METADATA_SEGMENT_CHANGE("segmentChange", "Segment Edited"), //
    METADATA_CHANGE("metadataChange", "Metadata Change"), //
    ACTIVITY_METRICS_CHANGE("activityMetricsChange", "Activity Metrics Change"); //

    private String name;
    private String displayName;
    private static final Set<ActionType> NON_WORKFLOW_JOB_TYPES = new HashSet<>(
            Arrays.asList(METADATA_CHANGE, RATING_ENGINE_CHANGE, METADATA_SEGMENT_CHANGE));

    private static Map<String, ActionType> actionTypeMap = new HashMap<>();

    static {
        for (ActionType type : values()) {
            actionTypeMap.put(type.getName(), type);
        }
    }

    private ActionType(String name) {
        this.name = name;
    }

    private ActionType(String name, String displayName) {
        this.name = name;
        this.displayName = displayName;
    }

    public String getName() {
        return this.name;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    public ActionType getActionType(String str) {
        return actionTypeMap.get(str);
    }

    public static Set<ActionType> getNonWorkflowActions() {
        return NON_WORKFLOW_JOB_TYPES;
    }
}
