package com.latticeengines.cdl.workflow;

import com.latticeengines.cdl.workflow.steps.export.RedshiftPublishStepConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class RedshiftPublishWorkflowConfiguration extends WorkflowConfiguration {

    private RedshiftPublishWorkflowConfiguration() {
    }

    public static class Builder {
        private RedshiftPublishWorkflowConfiguration configuration = new RedshiftPublishWorkflowConfiguration();

        private RedshiftPublishStepConfiguration redshiftExportStepConfiguration = new RedshiftPublishStepConfiguration();

        public Builder redshiftTableConfiguration(RedshiftTableConfiguration config) {
            redshiftExportStepConfiguration.setRedshiftTableConfiguration(config);
            return this;
        }

        public Builder bucketedTableName(String bucketedTableName) {
            redshiftExportStepConfiguration.setBucketedTableName(bucketedTableName);
            return this;
        }

        public RedshiftPublishWorkflowConfiguration build() {
            configuration.add(redshiftExportStepConfiguration);
            return configuration;
        }
    }
}
