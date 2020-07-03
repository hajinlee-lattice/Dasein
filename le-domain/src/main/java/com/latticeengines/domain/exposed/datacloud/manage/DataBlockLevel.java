package com.latticeengines.domain.exposed.datacloud.manage;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DataBlockLevel {
    L1("Level 1"), //
    L2("Level 2"), //
    L3("Level 3"), //
    L4("Level 4"), //
    L5("Level 5");

    private final String displayName;

    DataBlockLevel(String displayName) {
        this.displayName = displayName;
    }

    @JsonValue
    public String getDisplayName() {
        return displayName;
    }
}
