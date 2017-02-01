package com.latticeengines.domain.exposed.eai.route;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;

@JsonSubTypes({
        @JsonSubTypes.Type(value = SftpToHdfsRouteConfiguration.class, name = "SftpToHdfsRouteConfiguration"),
        @JsonSubTypes.Type(value = HdfsToS3Configuration.class, name = "HdfsToS3Configuration"),
        @JsonSubTypes.Type(value = HdfsToSnowflakeConfiguration.class, name = "HdfsToSnowflakeConfiguration"),
})
public class CamelRouteConfiguration extends EaiJobConfiguration{

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
