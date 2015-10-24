package com.latticeengines.domain.exposed.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.metadata.Table;

public class DataFlowSource implements HasName {

    private String name;
    private Table table;
    private String rawDataPath;
    
    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    
    @JsonProperty("table")
    public Table getTable() {
        return table;
    }
    
    @JsonProperty("table")
    public void setTable(Table table) {
        this.table = table;
    }

    @JsonProperty("raw_data_path")
    public String getRawDataPath() {
        return rawDataPath;
    }

    @JsonProperty("raw_data_path")
    public void setRawDataPath(String rawDataPath) {
        this.rawDataPath = rawDataPath;
    }

}