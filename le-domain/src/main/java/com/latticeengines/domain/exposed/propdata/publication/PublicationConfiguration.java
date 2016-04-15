package com.latticeengines.domain.exposed.propdata.publication;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "ConfigurationType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PublishToSqlConfiguration.class, name = "PublishToSqlConfiguration")
})
@JsonInclude(JsonInclude.Include.NON_NULL)
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

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
