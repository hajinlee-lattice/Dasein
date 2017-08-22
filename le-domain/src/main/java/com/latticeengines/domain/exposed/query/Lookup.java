package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.latticeengines.common.exposed.graph.GraphNode;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @Type(value = EntityLookup.class, name = "entity"), //
        @Type(value = AttributeLookup.class, name = "attribute"), //
        @Type(value = ColumnLookup.class, name = "column"), //
        @Type(value = ValueLookup.class, name = "value"), //
        @Type(value = RangeLookup.class, name = "range"), //
        @Type(value = CaseLookup.class, name = "case"), //
        @Type(value = SubQueryAttrLookup.class, name = "subquery"), //
        @Type(value = AggregateLookup.class, name = "aggregate")})
public abstract class Lookup implements GraphNode {

    @Override
    public Collection<? extends GraphNode> getChildren() {
        return new ArrayList<>();
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return new HashMap<>();
    }
}
