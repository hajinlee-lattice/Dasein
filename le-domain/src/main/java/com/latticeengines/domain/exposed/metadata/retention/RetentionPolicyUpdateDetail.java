package com.latticeengines.domain.exposed.metadata.retention;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RetentionPolicyUpdateDetail {

    @JsonProperty("TableNames")
    private List<String> tableNames;

    @JsonProperty("RetentionPolicy")
    private RetentionPolicy retentionPolicy;

    public RetentionPolicy getRetentionPolicy() {
        return retentionPolicy;
    }

    public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
        this.retentionPolicy = retentionPolicy;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }
}
