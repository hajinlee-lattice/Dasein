package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourcefileConfig {

    @JsonProperty("unique_identifier_config")
    private UniqueIdentifierConfig uniqueIdentifierConfig;

    public UniqueIdentifierConfig getUniqueIdentifierConfig() {
        return uniqueIdentifierConfig;
    }

    public void setUniqueIdentifierConfig(UniqueIdentifierConfig uniqueIdentifierConfig) {
        this.uniqueIdentifierConfig = uniqueIdentifierConfig;
    }
}
