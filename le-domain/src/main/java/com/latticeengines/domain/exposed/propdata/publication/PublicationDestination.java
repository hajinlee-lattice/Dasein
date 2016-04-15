package com.latticeengines.domain.exposed.propdata.publication;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "DestinationType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SqlDestination.class, name = "SqlDestination")
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class PublicationDestination {

    @JsonIgnore
    protected abstract String getDestinationType();

}
