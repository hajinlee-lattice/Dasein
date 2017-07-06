package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

/**
 * Entities satisfy the Restriction exists (if negate then not exists)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ExistsRestriction extends Restriction {

    @JsonProperty("object_type")
    private SchemaInterpretation objectType;

    @JsonProperty("entity")
    private BusinessEntity entity;

    @JsonProperty("negate")
    private boolean negate;

    @JsonProperty("restriction")
    private Restriction restriction;

    public ExistsRestriction(SchemaInterpretation objectType, boolean negate, Restriction restriction) {
        this.objectType = objectType;
        this.negate = negate;
        this.restriction = restriction;
    }

    public ExistsRestriction(SchemaInterpretation objectType) {
        this.objectType = objectType;
    }

    public ExistsRestriction(SchemaInterpretation objectType, boolean negate) {
        this.objectType = objectType;
        this.negate = negate;
    }

    private ExistsRestriction(BusinessEntity entity) {
        this.entity = entity;
    }

    private ExistsRestriction(BusinessEntity entity, boolean negate) {
        this(entity);
        this.negate = negate;
    }

    ExistsRestriction(BusinessEntity entity, boolean negate, Restriction restriction) {
        this(entity, negate);
        this.restriction = restriction;
    }

    public ExistsRestriction() {
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public boolean getNegate() {
        return negate;
    }

    public void setNegate(boolean negate) {
        this.negate = negate;
    }

    public SchemaInterpretation getObjectType() {
        return objectType;
    }

    public void setObjectType(SchemaInterpretation objectName) {
        this.objectType = objectName;
    }

    public Restriction getRestriction() {
        return restriction;
    }

    public void setRestriction(Restriction restriction) {
        this.restriction = restriction;
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @Override
    public Collection<? extends GraphNode> getChildren() {
        List<GraphNode> children = new ArrayList<>();
        children.add(restriction);
        return children;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("restrictions", Collections.singletonList(restriction));
        return map;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }
}
