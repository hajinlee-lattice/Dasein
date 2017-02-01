package com.latticeengines.domain.exposed.eai;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.eai.route.HdfsToS3Configuration;
import com.latticeengines.domain.exposed.eai.route.HdfsToSnowflakeConfiguration;
import com.latticeengines.domain.exposed.eai.route.SftpToHdfsRouteConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ @JsonSubTypes.Type(value = ImportConfiguration.class, name = "ImportConfiguration"),
        @JsonSubTypes.Type(value = ExportConfiguration.class, name = "ExportConfiguration"),
        @JsonSubTypes.Type(value = SftpToHdfsRouteConfiguration.class, name = "SftpToHdfsRouteConfiguration"),
        @JsonSubTypes.Type(value = HdfsToS3Configuration.class, name = "HdfsToS3Configuration"),
        @JsonSubTypes.Type(value = HdfsToSnowflakeConfiguration.class, name = "HdfsToSnowflakeConfiguration") })
public class EaiJobConfiguration extends BasePayloadConfiguration {

    protected Map<String, String> properties = new HashMap<>();

    @JsonIgnore
    public String getProperty(String key) {
        return properties.get(key);
    }

    @JsonProperty("properties")
    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonProperty("properties")
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void setProperty(String propertyName, String propertyValue) {
        properties.put(propertyName, propertyValue);
    }
}
