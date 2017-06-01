package com.latticeengines.cdl.workflow;

import java.util.List;

import com.latticeengines.cdl.workflow.steps.export.ExportDataToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class RedshiftPublishWorkflowConfiguration extends WorkflowConfiguration {

    private RedshiftPublishWorkflowConfiguration() {
    }

    public static class Builder {
        private RedshiftPublishWorkflowConfiguration configuration = new RedshiftPublishWorkflowConfiguration();

        private ExportDataToRedshiftConfiguration exportDataToRedshiftConfiguration = new ExportDataToRedshiftConfiguration();

        public Builder initialLoad(boolean initialLoad) {
            exportDataToRedshiftConfiguration.setInitialLoad(initialLoad);
            return this;
        }

        public Builder cleanupS3(boolean cleanupS3) {
            exportDataToRedshiftConfiguration.setCleanupS3(cleanupS3);
            return this;
        }

        public Builder sourceTables(List<Table> sourceTables) {
            exportDataToRedshiftConfiguration.setSourceTables(sourceTables);
            return this;
        }

        public RedshiftPublishWorkflowConfiguration build() {
            configuration.add(exportDataToRedshiftConfiguration);
            return configuration;
        }
    }
}
