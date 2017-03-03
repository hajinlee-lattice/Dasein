package com.latticeengines.domain.exposed.query;

import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ExistsRestriction extends Restriction {
    @JsonProperty("object_type")
    private SchemaInterpretation objectType;
    @JsonProperty("negate")
    private boolean negate;
    @JsonProperty("restrictions")
    private List<Restriction> restrictions;

    public ExistsRestriction(SchemaInterpretation objectType, boolean negate, List<Restriction> restrictions) {
        this.objectType = objectType;
        this.negate = negate;
        this.restrictions = restrictions;
    }

    public ExistsRestriction(SchemaInterpretation objectType) {
        this.objectType = objectType;
    }

    public ExistsRestriction(SchemaInterpretation objectType, boolean negate) {
        this.objectType = objectType;
        this.negate = negate;
    }

    public ExistsRestriction() {
    }

    public boolean getNegate() {
        return negate;
    }

    public void setNegate(boolean negate) {
        this.negate = negate;
    }

    public SchemaInterpretation getobjectName() {
        return objectType;
    }

    public void setobjectName(SchemaInterpretation objectName) {
        this.objectType = objectName;
    }

    public List<Restriction> getRestrictions() {
        return restrictions;
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
