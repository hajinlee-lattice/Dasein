package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
@Type(value = ConcreteRestriction.class, name = "concreteRestriction"),
        @Type(value = ExistsRestriction.class, name = "existsRestriction"),
        @Type(value = LogicalRestriction.class, name = "logicalRestriction"),
        @Type(value = BucketRestriction.class, name = "bucketRestriction") })
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class Restriction implements GraphNode {

}
