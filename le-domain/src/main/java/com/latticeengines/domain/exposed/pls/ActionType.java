package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

public enum ActionType {

    CDL_DATAFEED_IMPORT_WORKFLOW("cdlDataFeedImportWorkflow"), //
    CDL_OPERATION_WORKFLOW("cdlOperationWorkflow"), //
    METADATA_CHANGE("metadataChange"); //

    private String name;

    private static Map<String, ActionType> actionTypeMap = new HashMap<>();

    static {
        for (ActionType type : values()) {
            actionTypeMap.put(type.getName(), type);
        }
    }

    private ActionType(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public ActionType getActionType(String str) {
        return actionTypeMap.get(str);
    }
}
