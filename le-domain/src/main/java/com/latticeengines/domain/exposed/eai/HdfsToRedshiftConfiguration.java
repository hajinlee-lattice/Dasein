package com.latticeengines.domain.exposed.eai;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;

public class HdfsToRedshiftConfiguration extends ExportConfiguration {

    @JsonProperty("redshift_table_config")
    private RedshiftTableConfiguration redshiftTableConfiguration;

    @JsonProperty("concrete_table")
    private boolean concreteTable = false;

    @JsonProperty("append")
    private boolean append = false;

    @JsonProperty("json_path_prefix")
    private String jsonPathPrefix;

    public RedshiftTableConfiguration getRedshiftTableConfiguration() {
        return redshiftTableConfiguration;
    }

    public void setRedshiftTableConfiguration(RedshiftTableConfiguration redshiftTableConfiguration) {
        this.redshiftTableConfiguration = redshiftTableConfiguration;
    }

    public boolean isConcreteTable() {
        return concreteTable;
    }

    public void setConcreteTable(boolean concreteTable) {
        this.concreteTable = concreteTable;
    }

    public boolean isAppend() {
        return append;
    }

    public void setAppend(boolean append) {
        this.append = append;
    }

    public String getJsonPathPrefix() {
        return jsonPathPrefix;
    }

    public void setJsonPathPrefix(String jsonPathPrefix) {
        this.jsonPathPrefix = jsonPathPrefix;
    }

}
