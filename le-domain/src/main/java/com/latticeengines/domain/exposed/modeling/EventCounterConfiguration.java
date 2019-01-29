package com.latticeengines.domain.exposed.modeling;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class EventCounterConfiguration {
    @NotNull
    private String customer;

    /* parallel */
    @NotNull
    private String table;

    private boolean parallelEnabled;

    private String hdfsDirPath;

    private Map<String, String> properties = new HashMap<String, String>();;

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

    @JsonProperty("parallel_enabled")
    public boolean isParallelEnabled() {
        return parallelEnabled;
    }

    @JsonProperty("parallel_enabled")
    public void setParallelEnabled(boolean parallelEnabled) {
        this.parallelEnabled = parallelEnabled;
    }

    @JsonProperty(value = "hdfs_dir_path", required = false)
    public String getHdfsDirPath() {
        return hdfsDirPath;
    }

    @JsonProperty(value = "hdfs_dir_path", required = false)
    public void setHdfsDirPath(String hdfsDirPath) {
        this.hdfsDirPath = hdfsDirPath;
    }

    @JsonProperty("property")
    @JsonAnySetter
    public void setProperty(String propertyName, String propertyValue) {
        properties.put(propertyName, propertyValue);
    }

    @JsonProperty("property")
    public String getProperty(String propertyName) {
        return properties.get(propertyName);
    }

    @JsonProperty("property")
    @JsonAnyGetter
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
