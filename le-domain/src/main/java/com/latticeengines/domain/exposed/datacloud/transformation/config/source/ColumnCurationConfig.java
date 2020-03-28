package com.latticeengines.domain.exposed.datacloud.transformation.config.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn.Calculation;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class ColumnCurationConfig extends TransformerConfig {
    @JsonProperty("ColumnOperations")
    private Calculation[] columnOperations; // for column batch operations to apply

    public Calculation[] getColumnOperations() {
        return columnOperations;
    }

    public void setColumnOperations(Calculation[] columnOperations) {
        this.columnOperations = columnOperations;
    }
}
