package com.latticeengines.domain.exposed.modeling;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class DataProfileConfiguration {

    private String customer;
    private String table;
    private String metadataTable;
    private String samplePrefix;
    private String script;
    private String containerProperties;
    private List<String> excludeColumnList = new ArrayList<String>();
    private List<String> includeColumnList = new ArrayList<String>();
    private List<String> targets = new ArrayList<String>();
    
    private boolean parallelEnabled;
    
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

    @JsonProperty("sample_prefix")
    public String getSamplePrefix() {
        return samplePrefix;
    }

    @JsonProperty("sample_prefix")
    public void setSamplePrefix(String samplePrefix) {
        this.samplePrefix = samplePrefix;
    }

    @JsonProperty("script")
    public String getScript() {
        return script;
    }

    @JsonProperty("script")
    public void setScript(String script) {
        this.script = script;
    }

    @JsonProperty("containerProperties")
    public String getContainerProperties() {
        return containerProperties;
    }

    @JsonProperty("containerProperties")
    public void setContainerProperties(String containerProperties) {
        this.containerProperties = containerProperties;
    }

    @JsonProperty("targets")
    public List<String> getTargets() {
        return targets;
    }

    @JsonProperty("targets")
    public void setTargets(List<String> targets) {
        this.targets = targets;
    }

    @JsonProperty(value = "parallel_enabled", required = false)
    public boolean isParallelEnabled() {
        return parallelEnabled;
    }

    @JsonProperty(value = "parallel_enabled", required = false)
    public void setParallelEnabled(boolean parallelEnabled) {
        this.parallelEnabled = parallelEnabled;
    }
    
}
