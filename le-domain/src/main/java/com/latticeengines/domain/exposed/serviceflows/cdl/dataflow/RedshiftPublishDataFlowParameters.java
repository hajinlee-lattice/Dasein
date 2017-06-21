package com.latticeengines.domain.exposed.serviceflows.cdl.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;

public class RedshiftPublishDataFlowParameters extends DataFlowParameters {

    @JsonProperty("source_table")
    @SourceTableName
    public String sourceTable;

    @JsonProperty("redshift_table_config")
    public RedshiftTableConfiguration redshiftTableConfig;

    public RedshiftPublishDataFlowParameters(String sourceTable, RedshiftTableConfiguration redshiftTableConfig) {
        this.sourceTable = sourceTable;
        this.redshiftTableConfig = redshiftTableConfig;
    }

}
