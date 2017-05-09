package com.latticeengines.cdl.workflow.steps.export;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class ExportDataToRedshiftConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("redshift_table_config")
    private RedshiftTableConfiguration redshiftTableConfig;

    @JsonProperty("source_table")
    private Table sourceTable;

    public RedshiftTableConfiguration getRedshiftTableConfiguration() {
        return redshiftTableConfig;
    }

    public void setRedshiftTableConfiguration(RedshiftTableConfiguration redshiftTableConfig) {
        this.redshiftTableConfig = redshiftTableConfig;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(Table sourceTable) {
        this.sourceTable = sourceTable;
    }
}
