package com.latticeengines.domain.exposed.propdata.publication;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "DestinationType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SqlDestination.class, name = "SqlDestination")
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class PublicationDestination {

    @JsonIgnore
    protected abstract String getDestinationType();

}
