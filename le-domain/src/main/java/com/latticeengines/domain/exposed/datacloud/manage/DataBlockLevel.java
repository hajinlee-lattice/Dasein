package com.latticeengines.domain.exposed.datacloud.manage;

import java.util.NoSuchElementException;

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

    public static DataBlockLevel parseDataBlockLevel(String lvl) {
        switch (lvl.toLowerCase()) {
            case "l1":
            case "level 1":
                return L1;
            case "l2":
            case "level 2":
                return L2;
            case "l3":
            case "level 3":
                return L3;
            case "l4":
            case "level 4":
                return L4;
            case "l5":
            case "level 5":
                return L5;
            default:
                throw new NoSuchElementException("Cannot parse " + lvl + " to a DataBlockLevel");
        }
    }
}
