package com.latticeengines.domain.exposed.modeling;

public enum ModelingReservedField {
    Rating("Rating");

    public String displayName;

    ModelingReservedField(String displayName) {
        this.displayName = displayName;
    }
}
