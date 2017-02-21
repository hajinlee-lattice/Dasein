package com.latticeengines.common.exposed.query;

import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
@JsonIgnoreProperties(ignoreUnknown = true)
public class MathematicalLookup extends Lookup {
    @JsonProperty("references")
    private List<Object> references;
    @JsonProperty("interpretations")
    private List<ReferenceInterpretation> interpretations;
    @JsonProperty("transformations")
    private List<Transformation> transformations;
    /**
     * The formula to use. Each reference is delimited by $reference-offset. So
     * for example, to add two references, use $0 + $1.
     */
    @JsonProperty("formula")
    private String formula;

    public MathematicalLookup(List<Object> references, List<ReferenceInterpretation> interpretations,
            List<Transformation> transformations, String formula) {
        this.references = references;
        this.interpretations = interpretations;
        this.transformations = transformations;
        this.formula = formula;
    }

    public String getFormula() {
        return formula;
    }

    public void setFormula(String formula) {
        this.formula = formula;
    }

    public List<Transformation> getTransformations() {
        return transformations;
    }

    public void setTransformations(List<Transformation> transformations) {
        this.transformations = transformations;
    }

    public List<ReferenceInterpretation> getInterpretations() {
        return interpretations;
    }

    public void setInterpretations(List<ReferenceInterpretation> interpretations) {
        this.interpretations = interpretations;
    }

    public List<Object> getReferences() {
        return references;
    }

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
