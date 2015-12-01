package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonValue;

public enum JobStepType {

    // New type has to be at the end
    LOAD_DATA(0, "Load Data"), //
    MATCH_DATA(1, "Match Data"), //
    GENERATE_INSIGHTS(2, "Generate Insights"),
    CREATE_MODEL(3, "Create Global Model"),
    CREATE_GLOBAL_TARGET_MARKET(4, "Create Global Target Market");

    private JobStepType(int typeId, String type) {
        this.typeId = typeId;
        this.type = type;
    }

    private int typeId;
    private String type;

    public int getTypeId() {
        return typeId;
    }

    public String getType() {
        return type;
    }

    private static Map<String, JobStepType> typeMap = new HashMap<>();

    static {
        for (JobStepType type : values()) {
            typeMap.put(type.getType(), type);
        }
    }

    @JsonValue
    public String getName() {
        return StringUtils.capitalize(super.name().toLowerCase());
    }

    public static JobStepType getByType(String type) {
        return typeMap.get(type);
    }
}
