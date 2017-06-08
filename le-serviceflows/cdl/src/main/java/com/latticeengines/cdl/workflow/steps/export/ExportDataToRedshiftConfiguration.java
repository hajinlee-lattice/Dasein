package com.latticeengines.cdl.workflow.steps.export;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ExportDataToRedshiftConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("redshift_table_config")
    private HdfsToRedshiftConfiguration hdfsToRedshiftConfiguration;

    @JsonProperty("source_table")
    private Table sourceTable;

    public HdfsToRedshiftConfiguration getHdfsToRedshiftConfiguration() {
        return hdfsToRedshiftConfiguration;
    }

    public void setHdfsToRedshiftConfiguration(HdfsToRedshiftConfiguration hdfsToRedshiftConfiguration) {
        this.hdfsToRedshiftConfiguration = hdfsToRedshiftConfiguration;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(Table sourceTable) {
        this.sourceTable = sourceTable;
    }
}
