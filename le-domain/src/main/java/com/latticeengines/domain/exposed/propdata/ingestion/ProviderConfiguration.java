package com.latticeengines.domain.exposed.propdata.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "ClassName")
@JsonSubTypes({ @JsonSubTypes.Type(value = SftpConfiguration.class, name = "SftpConfiguration"),
                @JsonSubTypes.Type(value = SqlToTextConfiguration.class, name = "SqlToTextConfiguration")
              })
public abstract class ProviderConfiguration {
    private String className;
    protected Integer concurrentNum;

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

    @JsonProperty("ConcurrentNum")
    public Integer getConcurrentNum() {
        return concurrentNum;
    }

    @JsonProperty("ConcurrentNum")
    public void setConcurrentNum(Integer concurrentNum) {
        this.concurrentNum = concurrentNum;
    }
}
