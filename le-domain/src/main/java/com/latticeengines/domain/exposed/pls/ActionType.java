package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

public enum ActionType {

    CDL_DATAFEED_IMPORT_WORKFLOW("cdlDataFeedImportWorkflow", "Import"), //
    CDL_OPERATION_WORKFLOW("cdlOperationWorkflow", "Delete"), //
    RATING_ENGINE_CHANGE("ratingEngineChange", "Scoring"), //
    METADATA_SEGMENT_CHANGE("segmentChange", "Segment Edited"), //
    METADATA_CHANGE("metadataChange", "Metadata Change"); //

    private String name;
    private String displayName;

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
}
