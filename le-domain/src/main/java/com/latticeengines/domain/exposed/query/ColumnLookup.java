package com.latticeengines.domain.exposed.query;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnLookup extends Lookup {
    @JsonProperty("column_name")
    private String columnName;
    @JsonProperty("object_type")
    private SchemaInterpretation objectType;

    public ColumnLookup(String columnName) {
        this.columnName = columnName;
    }

    public ColumnLookup(SchemaInterpretation objectType, String columnName) {
        this.objectType = objectType;
        this.columnName = columnName;
    }

    public ColumnLookup() {
    }

    public SchemaInterpretation getObjectType() {
        return objectType;
    }

    public void setObjectType(SchemaInterpretation objectType) {
        this.objectType = objectType;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
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
