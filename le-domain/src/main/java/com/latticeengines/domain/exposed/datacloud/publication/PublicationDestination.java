package com.latticeengines.domain.exposed.datacloud.publication;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;


/**
 * Each progress has a different destination
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "DestinationType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SqlDestination.class, name = "SqlDestination"),
        @JsonSubTypes.Type(value = DynamoDestination.class, name = "DynamoDestination")
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class PublicationDestination {

    @JsonIgnore
    protected abstract String getDestinationType();

}
