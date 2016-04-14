package com.latticeengines.domain.exposed.propdata.publication;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "ConfigurationType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PublishToSqlConfiguration.class, name = "PublishToSqlConfiguration")
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class PublicationConfiguration {

    protected PublicationDestination destination;

    @JsonIgnore
    protected abstract String getConfigurationType();

    @JsonProperty("Destination")
    public PublicationDestination getDestination() {
        return destination;
    }

    @JsonProperty("Destination")
    public void setDestination(PublicationDestination destination) {
        this.destination = destination;
    }
}
