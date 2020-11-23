package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourcefileConfig {

    @JsonProperty
    private boolean uniqueIdentifierRequired;

    @JsonProperty
    private int uniqueIdentifierLength;

    @JsonProperty
    private String uniqueIdentifierName;

    public boolean isUniqueIdentifierRequired() {
        return uniqueIdentifierRequired;
    }

    public void setUniqueIdentifierRequired(boolean uniqueIdentifierRequired) {
        this.uniqueIdentifierRequired = uniqueIdentifierRequired;
    }

    public int getUniqueIdentifierLength() {
        return uniqueIdentifierLength;
    }

    public void setUniqueIdentifierLength(int uniqueIdentifierLength) {
        this.uniqueIdentifierLength = uniqueIdentifierLength;
    }

    public String getUniqueIdentifierName() {
        return uniqueIdentifierName;
    }

    public void setUniqueIdentifierName(String uniqueIdentifierName) {
        this.uniqueIdentifierName = uniqueIdentifierName;
    }

    public boolean isUniqueIdentifierChanged() {
        return uniqueIdentifierLength > 0 || uniqueIdentifierRequired;
    }
}
