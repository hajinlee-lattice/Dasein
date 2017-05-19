package com.latticeengines.domain.exposed.validation;

public enum ReservedField {

    Percentile("Score"),
    Rating("Rating");

    public String displayName;

    ReservedField(String displayName) {
        this.displayName = displayName;
    }
}
