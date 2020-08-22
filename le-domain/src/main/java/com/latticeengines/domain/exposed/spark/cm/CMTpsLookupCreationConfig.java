package com.latticeengines.domain.exposed.spark.cm;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CMTpsLookupCreationConfig extends SparkJobConfig {

    public static final String NAME = "CMTpsLookupCreation";

    @JsonProperty("Key")
    String key;

    @JsonProperty("TargetColumn")
    String targetColumn;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getTargetColumn() {
        return targetColumn;
    }

    public void setTargetColumn(String targetColumn) {
        this.targetColumn = targetColumn;
    }
}
