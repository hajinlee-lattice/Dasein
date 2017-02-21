package com.latticeengines.domain.exposed.eai;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.source.SourceCredentialType;

public class SourceImportConfiguration {

    @JsonProperty("source_type")
    private SourceType sourceType;
    
    @JsonProperty("tables")
    private List<Table> tables;
    
    @JsonProperty("filters")
    private Map<String, String> filters = new HashMap<>();
    
    @JsonProperty("properties")
    private Map<String, String> properties = new HashMap<>();
    
    @JsonProperty("source_cred_type")
    private SourceCredentialType sourceCredentialType = SourceCredentialType.PRODUCTION;

    
    public SourceType getSourceType() {
        return sourceType;
    }

    public void setSourceType(SourceType sourceType) {
        this.sourceType = sourceType;
    }

    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public void setFilter(String tableName, String expression) {
        filters.put(tableName, expression);
    }

    
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

    
    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void setProperty(String propertyName, String propertyValue) {
        properties.put(propertyName, propertyValue);
    }

    public SourceCredentialType getSourceCredentialType() {
        return sourceCredentialType;
    }

    public void setSourceCredentialType(SourceCredentialType sourceCredentialType) {
        this.sourceCredentialType = sourceCredentialType;
    }
}
