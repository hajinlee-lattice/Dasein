package com.latticeengines.domain.exposed.eai;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class DataExtractionConfiguration implements HasName {
    
    private String name;
    private List<Table> tables;
    private Map<String, String> filters = new HashMap<>();

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

    @JsonProperty("tables")
    public List<Table> getTables() {
        return tables;
    }

    @JsonProperty("tables")
    public void setTables(List<Table> tables) {
        this.tables = tables;
    }
    
    public void putFilter(String tableName, String expression) {
        filters.put(tableName, expression);
    }
    
    @JsonProperty("filters")
    public Map<String, String> getFilters() {
        return filters;
    }
    
    public String getFilter(String tableName) {
        return filters.get(tableName);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    
}
