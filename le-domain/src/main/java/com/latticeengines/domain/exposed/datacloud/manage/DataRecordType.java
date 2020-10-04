package com.latticeengines.domain.exposed.datacloud.manage;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DataRecordType {

    Domain("Domain Use"), //
    MasterData("Domain Master Data Use"), //
    Analytical("Analytical Use");

    private final String displayName;

    private static Map<String, DataRecordType> displayNameMap = new HashMap<>();
    static {
        for (DataRecordType recordType: DataRecordType.values()) {
            displayNameMap.put(recordType.getDisplayName().toLowerCase(), recordType);
        }
    }

    DataRecordType(String displayName) {
        this.displayName = displayName;
    }

    @JsonValue
    public String getDisplayName() {
        return displayName;
    }

    public static DataRecordType parse(String str) {
        if (displayNameMap.containsKey(str.toLowerCase())) {
            return displayNameMap.get(str.toLowerCase());
        } else {
            return DataRecordType.valueOf(str);
        }
    }

}
