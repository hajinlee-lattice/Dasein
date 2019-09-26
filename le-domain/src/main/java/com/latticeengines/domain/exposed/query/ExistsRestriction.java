package com.latticeengines.domain.exposed.query;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
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

    @JsonProperty("entity")
    private BusinessEntity entity;

    @JsonProperty("negate")
    private boolean negate;

    @JsonProperty("restriction")
    private Restriction restriction;

    @JsonIgnore
    private Object subQueryExpression; // to be cased to query dsl SQLQuery<?>

    @Deprecated
    public ExistsRestriction(SchemaInterpretation objectType, boolean negate) {
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

    public Restriction getRestriction() {
        return restriction;
    }

    public void setRestriction(Restriction restriction) {
        this.restriction = restriction;
    }

    public Object getSubQueryExpression() {
        return subQueryExpression;
    }

    public void setSubQueryExpression(Object subQueryExpression) {
        this.subQueryExpression = subQueryExpression;
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
        return Collections.emptyList();
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return Collections.emptyMap();
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }
}
