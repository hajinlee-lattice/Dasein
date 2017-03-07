package com.latticeengines.cdl.workflow.steps.export;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class RedshiftPublishStepConfiguration extends DataFlowStepConfiguration {

    @JsonProperty("redshiftTableConfig")
    private RedshiftTableConfiguration redshiftTableConfig;

    @JsonProperty("bucketedTableName")
    private String bucketedTableName;

    public RedshiftPublishStepConfiguration() {
        setBeanName("redshiftPublishDataflow");
    }

    public RedshiftTableConfiguration getRedshiftTableConfiguration() {
        return redshiftTableConfig;
    }

    public void setRedshiftTableConfiguration(RedshiftTableConfiguration redshiftTableConfig) {
        this.redshiftTableConfig = redshiftTableConfig;
    }

    public String getBucketedTableName() {
        return bucketedTableName;
    }

    public void setBucketedTableName(String bucketedTableName) {
        this.bucketedTableName = bucketedTableName;
    }
}
