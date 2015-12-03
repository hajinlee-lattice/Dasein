package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonValue;

public enum JobType {

    // New type has to be at the end
    LOADFILE_IMPORT(0, "Loadfile Import"), //
    SFDC_IMPORT(1, "Salesforce Import"), //
    QUOTA_FLOW(2, "Quota Flow");

    private JobType(int typeId, String type) {
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

    private static Map<String, JobType> typeMap = new HashMap<>();

    static {
        for (JobType type : values()) {
            typeMap.put(type.getType(), type);
        }
    }

    @JsonValue
    public String getName() {
        return StringUtils.capitalize(super.name().toLowerCase());
    }

    public static JobType getByType(String type) {
        return typeMap.get(type);
    }

}
