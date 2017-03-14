package com.latticeengines.cdl.workflow.steps.export;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class RedshiftPublishStepConfiguration extends DataFlowStepConfiguration {

    @JsonProperty("redshiftTableConfig")
    private RedshiftTableConfiguration redshiftTableConfig;

    @JsonProperty("sourceTable")
    private Table sourceTable;

    public RedshiftPublishStepConfiguration() {
        setBeanName("redshiftPublishDataflow");
    }

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
