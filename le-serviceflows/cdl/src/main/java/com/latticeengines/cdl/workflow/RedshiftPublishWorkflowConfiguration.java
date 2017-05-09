package com.latticeengines.cdl.workflow;

import com.latticeengines.cdl.workflow.steps.export.ExportDataToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class RedshiftPublishWorkflowConfiguration extends WorkflowConfiguration {

    private RedshiftPublishWorkflowConfiguration() {
    }

    public static class Builder {
        private RedshiftPublishWorkflowConfiguration configuration = new RedshiftPublishWorkflowConfiguration();

        private ExportDataToRedshiftConfiguration exportDataToRedshiftConfiguration = new ExportDataToRedshiftConfiguration();

        public Builder redshiftTableConfiguration(RedshiftTableConfiguration config) {
            exportDataToRedshiftConfiguration.setRedshiftTableConfiguration(config);
            return this;
        }

        public Builder sourceTable(Table sourceTable) {
            exportDataToRedshiftConfiguration.setSourceTable(sourceTable);
            return this;
        }

        public RedshiftPublishWorkflowConfiguration build() {
            configuration.add(exportDataToRedshiftConfiguration);
            return configuration;
        }
    }
}
