package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class FilterChangelistConfig extends SparkJobConfig {
    public static final String NAME = "filterChangelist";

    @JsonProperty("Key")
    private String key;

    @JsonProperty("ColumnId")
    private String columnId;

    @JsonProperty("SelectColumns")
    private List<String> selectColumns;

    @Override
    public String getName() {
        return NAME;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getColumnId() {
        return columnId;
    }

    public void setColumnId(String columnId) {
        this.columnId = columnId;
    }

    public List<String> getSelectColumns() {
        return selectColumns;
    }

    public void setSelectColumns(List<String> selectColumns) {
        this.selectColumns = selectColumns;
    }

    @Override
    public int getNumTargets() {
        return 2;
    }
}
