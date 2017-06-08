package com.latticeengines.domain.exposed.serviceflows.datacloud.etl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.EngineConstants;
import com.latticeengines.domain.exposed.datacloud.ingestion.ProviderConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.IngestionStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class IngestionWorkflowConfiguration extends WorkflowConfiguration {
    public static class Builder {
        private IngestionWorkflowConfiguration ingestionWorkflowConfig = new IngestionWorkflowConfiguration();
        private IngestionStepConfiguration ingestionConfig = new IngestionStepConfiguration();

        public Builder ingestionProgress(IngestionProgress progress) {
            ingestionConfig.setIngestionProgress(progress);
            return this;
        }

        public Builder ingestion(Ingestion ingestion) {
            ingestionConfig.setIngestion(ingestion);
            return this;
        }

        public Builder providerConfiguration(ProviderConfiguration providerConfiguration) {
            ingestionConfig.setProviderConfiguration(providerConfiguration);
            return this;
        }

        public IngestionWorkflowConfiguration build() {
            ingestionWorkflowConfig.setContainerConfiguration("ingestionWorkflow",
                    EngineConstants.PRODATA_CUSTOMERSPACE, "IngestionWorkflow");
            ingestionWorkflowConfig
                    .setCustomerSpace(CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
            ingestionWorkflowConfig.add(ingestionConfig);
            return ingestionWorkflowConfig;
        }
    }
}
