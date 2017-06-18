package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.graph.GraphNode;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @Type(value = ConcreteRestriction.class, name = "concrete"),
        @Type(value = ExistsRestriction.class, name = "exists"),
        @Type(value = LogicalRestriction.class, name = "logical") })
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class Restriction implements GraphNode {

    Restriction(){}

    public static RestrictionBuilder builder() {
        return new RestrictionBuilder();
    }

}
