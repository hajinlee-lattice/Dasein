package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class SelectByColumnConfig extends SparkJobConfig {

    public static final String NAME = "selectByColumn";

    @JsonProperty("source_column")
    private String sourceColumn;

    @JsonProperty("dest_column")
    private String destColumn;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public String getSourceColumn() {
        return sourceColumn;
    }

    public void setSourceColumn(String sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    public String getDestColumn() {
        return destColumn;
    }

    public void setDestColumn(String destColumn) {
        this.destColumn = destColumn;
    }
}
