package com.latticeengines.domain.exposed.datacloud.manage;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DataRecordType {

    Domain("Domain"), //
    MasterData("MasterData"), //
    Analytical("Analytical");

    private final String displayName;

    DataRecordType(String displayName) {
        this.displayName = displayName;
    }

    @JsonValue
    public String getDisplayName() {
        return displayName;
    }

}
