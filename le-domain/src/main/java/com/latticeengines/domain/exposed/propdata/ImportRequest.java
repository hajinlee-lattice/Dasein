package com.latticeengines.domain.exposed.propdata;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ImportRequest extends SqoopRequest {

    private String splitColumn, whereClause;

    @JsonProperty("SplitColumn")
    public String getSplitColumn() {
        return splitColumn;
    }

    @JsonProperty("SplitColumn")
    public void setSplitColumn(String splitColumn) {
        this.splitColumn = splitColumn;
    }

    @JsonProperty("WhereClause")
    public String getWhereClause() {
        return whereClause;
    }

    @JsonProperty("WhereClause")
    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }
}
