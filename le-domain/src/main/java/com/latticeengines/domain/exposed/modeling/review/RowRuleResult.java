package com.latticeengines.domain.exposed.modeling.review;

import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;

public class RowRuleResult extends BaseRuleResult {

    private Map<String, Set<AttributeMetadata>> rowIdBecauseOfColumns;

    @JsonProperty
    public Map<String, Set<AttributeMetadata>> getRowIdBecauseOfColumns() {
        return rowIdBecauseOfColumns;
    }

    @JsonProperty
    public void setRowIdBecauseOfColumns(Map<String, Set<AttributeMetadata>> rowIdBecauseOfColumns) {
        this.rowIdBecauseOfColumns = rowIdBecauseOfColumns;
    }
}
