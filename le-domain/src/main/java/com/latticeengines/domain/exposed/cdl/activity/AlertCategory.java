package com.latticeengines.domain.exposed.cdl.activity;

public enum AlertCategory {
    PRODUCTS("Products"), //
    PEOPLE("People");

    private String displayName;

    AlertCategory(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }

}
