package com.latticeengines.domain.exposed.datacloud.publication;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "ConfigurationType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PublishToSqlConfiguration.class, name = "PublishToSqlConfiguration"),
        @JsonSubTypes.Type(value = PublishTextToSqlConfiguration.class, name = "PublishTextToSqlConfiguration")
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class PublicationConfiguration {

    @JsonProperty("SourceName")
    private String sourceName;

    @JsonProperty("Destination")
    protected PublicationDestination destination;

    @JsonProperty("AvroDir")
    private String avroDir;

    @JsonIgnore
    protected abstract String getConfigurationType();

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public PublicationDestination getDestination() {
        return destination;
    }

    public void setDestination(PublicationDestination destination) {
        this.destination = destination;
    }

    public String getAvroDir() {
        return avroDir;
    }

    public void setAvroDir(String avroDir) {
        this.avroDir = avroDir;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
