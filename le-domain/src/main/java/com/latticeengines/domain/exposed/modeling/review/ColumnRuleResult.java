package com.latticeengines.domain.exposed.modeling.review;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;

public class ColumnRuleResult extends BaseRuleResult {

    private Set<AttributeMetadata> columns;

    @JsonProperty
    public Set<AttributeMetadata> getColumnNames() {
        return columns;
    }

    @JsonProperty
    public void setColumnNames(Set<AttributeMetadata> columnNames) {
        this.columns = columnNames;
    }
}
