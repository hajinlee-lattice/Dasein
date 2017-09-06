package com.latticeengines.domain.exposed.datacloud.manage;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Column {

    private String externalColumnId;
    private String columnName;

    public Column() {
    }

    public Column(String externalColumnId) {
        this(externalColumnId, externalColumnId);
    }

    public Column(String externalColumnId, String columnName) {
        setExternalColumnId(externalColumnId);
        setColumnName(columnName);
    }

    @JsonProperty("ExternalColumnID")
    public String getExternalColumnId() {
        return externalColumnId;
    }

    @JsonProperty("ExternalColumnID")
    public void setExternalColumnId(String externalColumnId) {
        this.externalColumnId = externalColumnId;
    }

    @JsonProperty("ColumnName")
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty("ColumnName")
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public boolean equals(Object that) {
        if (!(that instanceof Column)) {
            return false;
        } else if (that == this) {
            return true;
        }

        Column rhs = (Column) that;
        return new EqualsBuilder() //
                .append(externalColumnId, rhs.getExternalColumnId()) //
                .append(columnName, rhs.getColumnName()) //
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(3, 11).append(externalColumnId).append(columnName).toHashCode();
    }

}
