package com.latticeengines.common.exposed.query;


import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.latticeengines.common.exposed.graph.GraphNode;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=As.WRAPPER_OBJECT, property="property")
@JsonSubTypes({
    @Type(value=ConcreteRestriction.class, name="concreteRestriction"),
    @Type(value=ExistsRestriction.class, name="existsRestriction"),
    @Type(value=LogicalRestriction.class, name="logicalRestriction"),
})
public abstract class Restriction implements GraphNode {

    @Override
    public List<GraphNode> getChildren() {
        return new ArrayList<GraphNode>();
    }

}
