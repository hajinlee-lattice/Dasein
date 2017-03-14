package com.latticeengines.domain.exposed.query;

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

    public ExistsRestriction() {
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

}
