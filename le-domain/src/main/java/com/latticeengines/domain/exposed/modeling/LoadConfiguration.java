package com.latticeengines.domain.exposed.modeling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class LoadConfiguration {

    private String table;
    private String metadataTable;
    private String customer;
    private DbCreds creds;
    private List<String> keyCols = new ArrayList<String>();
    private Map<String, String> properties = new HashMap<>();
    private String query;
    private String targetHdfsDir;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @JsonProperty("metadata_table")
    public String getMetadataTable() {
        return metadataTable;
    }

    @JsonProperty("metadata_table")
    public void setMetadataTable(String metadataTable) {
        this.metadataTable = metadataTable;
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public DbCreds getCreds() {
        return creds;
    }

    public void setCreds(DbCreds creds) {
        this.creds = creds;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonProperty("key_columns")
    public List<String> getKeyCols() {
        return keyCols;
    }

    @JsonProperty("key_columns")
    public void setKeyCols(List<String> keyCols) {
        this.keyCols = keyCols;
    }

    @JsonProperty(value = "properties", required = false)
    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonProperty(value = "properties", required = false)
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
    
    @JsonProperty(value = "query", required = false)
    public void setQuery(String query) {
        this.query = query;
    }
    
    @JsonProperty(value = "query", required = false)
    public String getQuery() {
        return query;
    }

    public void setProperty(String propertyName, String propertyValue) {
        properties.put(propertyName, propertyValue);
    }

    @JsonProperty(value = "target_dir", required = false)
    public String getTargetHdfsDir() {
        return targetHdfsDir;
    }

    @JsonProperty(value = "target_dir", required = false)
    public void setTargetHdfsDir(String targetHdfsDir) {
        this.targetHdfsDir = targetHdfsDir;
    }
}
