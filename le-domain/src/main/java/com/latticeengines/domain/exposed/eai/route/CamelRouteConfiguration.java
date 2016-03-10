package com.latticeengines.domain.exposed.eai.route;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "class_name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SftpToHdfsRouteConfiguration.class, name = "SftpToHdfsRouteConfiguration")
})
public class CamelRouteConfiguration {

    private String className;

    public CamelRouteConfiguration() {
        setClassName(getClass().getSimpleName());
    }

    @JsonProperty("class_name")
    private String getClassName() {
        return className;
    }

    @JsonProperty("class_name")
    private void setClassName(String className) {
        this.className = className;
    }
}
