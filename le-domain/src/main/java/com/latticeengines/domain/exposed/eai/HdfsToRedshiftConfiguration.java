package com.latticeengines.domain.exposed.eai;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;

public class HdfsToRedshiftConfiguration extends ExportConfiguration {

    @JsonProperty("redshift_table_config")
    @NotNull
    private RedshiftTableConfiguration redshiftTableConfiguration;

    @JsonProperty("concrete_table")
    private boolean concreteTable = false;

    @JsonProperty("append")
    private boolean append = false;

    @JsonProperty("directly_copy")
    private boolean directlyCopy = false;

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

    public boolean isDirectlyCopy() {
        return directlyCopy;
    }

    public void setDirectlyCopy(boolean directlyCopy) {
        this.directlyCopy = directlyCopy;
    }

}
