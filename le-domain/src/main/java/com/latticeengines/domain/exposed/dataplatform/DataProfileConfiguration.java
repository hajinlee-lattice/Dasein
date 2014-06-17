package com.latticeengines.domain.exposed.dataplatform;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.common.exposed.util.JsonUtils;

public class DataProfileConfiguration {

    private String customer;
    private String table;
    private String metadataTable;
    private List<String> excludeColumnList = new ArrayList<String>();
    private List<String> includeColumnList = new ArrayList<String>();
    
    @JsonProperty("customer")
    public String getCustomer() {
        return customer;
    }
    
    @JsonProperty("customer")
    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @JsonProperty("table")
    public String getTable() {
        return table;
    }

    @JsonProperty("table")
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

    @JsonProperty("exclude_list")
    public List<String> getExcludeColumnList() {
        return excludeColumnList;
    }

    @JsonProperty("exclude_list")
    public void setExcludeColumnList(List<String> excludeColumnList) {
        this.excludeColumnList = excludeColumnList;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonProperty("include_list")
    public List<String> getIncludeColumnList() {
        return includeColumnList;
    }

    @JsonProperty("include_list")
    public void setIncludeColumnList(List<String> includeColumnList) {
        this.includeColumnList = includeColumnList;
    }

    
}
