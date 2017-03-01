package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LogicalRestriction extends Restriction {
    @JsonProperty("operator")
    private LogicalOperator operator;
    @JsonProperty("restrictions")
    private List<Restriction> restrictions;

    public LogicalRestriction(LogicalOperator operator, List<Restriction> restrictions) {
        this.operator = operator;
        this.restrictions = restrictions;
    }

    public LogicalRestriction() {
        this.restrictions = new ArrayList<>();
    }

    public LogicalOperator getOperator() {
        return operator;
    }

    public void setOperator(LogicalOperator operator) {
        this.operator = operator;
    }

    public List<Restriction> getRestrictions() {
        return restrictions;
    }

    public void addRestriction(Restriction restriction) {
        restrictions.add(restriction);
    }

    public void setRestrictions(List<Restriction> restrictions) {
        this.restrictions = restrictions;
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

}
