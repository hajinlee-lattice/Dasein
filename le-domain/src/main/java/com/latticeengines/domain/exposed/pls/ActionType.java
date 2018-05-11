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
    ACTIVITY_METRICS_CHANGE("activityMetricsChange", "Activity Metrics Change"), //
    // Attribute Management related type
    ATTRIBUTE_MANAGEMENT_ACTIVATION("attributeManagementActivation", "Attributes Activated"), //
    ATTRIBUTE_MANAGEMENT_DEACTIVATION("attributeManagementDeactivation", "Attributes Deactivated"), //

    @Deprecated
    DISPLAY_PROPERTY_CHANGE("displayPropertyChange", "Display Property Change"), //
    @Deprecated
    USAGE_PROPERTY_CHANGE("usagePropertyChange", "Usage Property Change"), //
    @Deprecated
    LIFE_CYCLE_CHANGE("lifeCycleChange", "Life Cycle Change");

    private String name;
    private String displayName;
    private static final Set<ActionType> NON_WORKFLOW_JOB_TYPES = new HashSet<>(Arrays.asList(METADATA_CHANGE,
            RATING_ENGINE_CHANGE, METADATA_SEGMENT_CHANGE, ATTRIBUTE_MANAGEMENT_ACTIVATION,
            ATTRIBUTE_MANAGEMENT_DEACTIVATION, DISPLAY_PROPERTY_CHANGE, USAGE_PROPERTY_CHANGE, LIFE_CYCLE_CHANGE));

    private static final Set<ActionType> ATTR_MANAGEMENT_TYPES = new HashSet<>(
            Arrays.asList(DISPLAY_PROPERTY_CHANGE, USAGE_PROPERTY_CHANGE, LIFE_CYCLE_CHANGE,
                    ATTRIBUTE_MANAGEMENT_ACTIVATION, ATTRIBUTE_MANAGEMENT_DEACTIVATION));

    private static final Set<ActionType> RATING_RELATED_TYPES = new HashSet<>(
            Arrays.asList(RATING_ENGINE_CHANGE, METADATA_SEGMENT_CHANGE));

    private static Map<String, ActionType> actionTypeMap = new HashMap<>();

    static {
        for (ActionType type : values()) {
            actionTypeMap.put(type.getName(), type);
        }
    }

    ActionType(String name, String displayName) {
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

    public static Set<ActionType> getAttrManagementTypes() {
        return ATTR_MANAGEMENT_TYPES;
    }

    public static Set<ActionType> getRatingRelatedTypes() {
        return RATING_RELATED_TYPES;
    }
}
