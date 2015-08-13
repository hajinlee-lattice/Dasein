package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.List;

/**
 * Specifies one or more column or value lookups in a query.
 *  MAX(ColumnName) would be {interpretation = [COLUMN], references = [ColumnName], transformations = [MAX]}
 *  MAX(ColumnName) + 5 would be {interpretation = [COLUMN, VALUE], references = [ColumnName, 5], transformations = [MAX, null], formula = $0 + $1}
 */
public class Lookup {

    public Lookup(List<Object> references, List<ReferenceInterpretation> interpretations, List<Transformation> transformations, String formula) {
        this.references = references;
        this.interpretations = interpretations;
        this.transformations = transformations;
        this.formula = formula;
    }

    /**
     * Serialization constructor
     */
    @Deprecated
    public Lookup() {
    }

    @JsonProperty
    public List<Object> references;

    @JsonProperty
    public List<ReferenceInterpretation> interpretations;

    @JsonProperty
    public List<Transformation> transformations;

    /**
     * The formula to use.  Each reference is delimited by $reference-offset.
     * So for example, to add two references, use $0 + $1.
     */
    @JsonProperty
    public String formula;

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
