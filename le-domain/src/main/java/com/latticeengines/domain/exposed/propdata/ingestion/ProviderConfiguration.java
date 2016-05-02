package com.latticeengines.domain.exposed.propdata.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "ClassName")
@JsonSubTypes({ @JsonSubTypes.Type(value = SftpConfiguration.class, name = "SftpProtocol") })
public abstract class ProviderConfiguration {
    private String className;

    public ProviderConfiguration() {
        setClassName(getClass().getSimpleName());
    }

    @JsonProperty("ClassName")
    private String getClassName() {
        return className;
    }

    @JsonProperty("ClassName")
    private void setClassName(String className) {
        this.className = className;
    }
}
