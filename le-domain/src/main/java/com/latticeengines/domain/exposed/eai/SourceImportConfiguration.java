package com.latticeengines.domain.exposed.eai;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class SourceImportConfiguration {

    private SourceType sourceType;
    private List<Table> tables;
    private Map<String, String> filters = new HashMap<>();
    private Map<String, String> properties = new HashMap<>();
    
    @JsonProperty("source_type")
    public SourceType getSourceType() {
        return sourceType;
    }

    @JsonProperty("source_type")
    public void setSourceType(SourceType sourceType) {
        this.sourceType = sourceType;
    }

    @JsonProperty("tables")
    public List<Table> getTables() {
        return tables;
    }

    @JsonProperty("tables")
    public void setTables(List<Table> tables) {
        this.tables = tables;
    }
    
    public void setFilter(String tableName, String expression) {
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

    @JsonProperty("properties")
    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonProperty("properties")
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void setProperty(String propertyName, String propertyValue) {
        filters.put(propertyName, propertyValue);
    }
    
    
}
