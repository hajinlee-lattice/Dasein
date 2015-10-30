package com.latticeengines.common.exposed.query;

import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Specifies one or more column or value lookups in a query.
 * 
 * <p>
 * MAX(ColumnName) would be {interpretation = [COLUMN], references =
 * [ColumnName], transformations = [MAX]}
 * 
 * <p>
 * MAX(ColumnName) + 5 would be {interpretation = [COLUMN, VALUE], references =
 * [ColumnName, 5], transformations = [MAX, null], formula = $0 + $1}
 */
public class MathematicalLookup extends Lookup {
    private List<Object> references;
    private List<ReferenceInterpretation> interpretations;
    private List<Transformation> transformations;
    /**
     * The formula to use. Each reference is delimited by $reference-offset. So
     * for example, to add two references, use $0 + $1.
     */
    private String formula;

    public MathematicalLookup(List<Object> references, List<ReferenceInterpretation> interpretations,
            List<Transformation> transformations, String formula) {
        this.references = references;
        this.interpretations = interpretations;
        this.transformations = transformations;
        this.formula = formula;
    }

    @JsonProperty("formula")
    public String getFormula() {
        return formula;
    }

    @JsonProperty("formula")
    public void setFormula(String formula) {
        this.formula = formula;
    }

    @JsonProperty("transformations")
    public List<Transformation> getTransformations() {
        return transformations;
    }

    @JsonProperty("transformations")
    public void setTransformations(List<Transformation> transformations) {
        this.transformations = transformations;
    }

    @JsonProperty("interpretations")
    public List<ReferenceInterpretation> getInterpretations() {
        return interpretations;
    }

    @JsonProperty("interpretations")
    public void setInterpretations(List<ReferenceInterpretation> interpretations) {
        this.interpretations = interpretations;
    }

    @JsonProperty("references")
    public List<Object> getReferences() {
        return references;
    }

    @JsonProperty("references")
    public void setReferences(List<Object> references) {
        this.references = references;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public MathematicalLookup() {
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
