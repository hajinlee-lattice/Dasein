package com.latticeengines.propdata.workflow.engine;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.propdata.ingestion.Protocol;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.propdata.core.PropDataConstants;
import com.latticeengines.propdata.workflow.engine.steps.EngineConstants;
import com.latticeengines.propdata.workflow.engine.steps.IngestionStepConfiguration;

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

        public Builder protocol(Protocol protocol) {
            ingestionConfig.setProtocol(protocol);
            return this;
        }

        public IngestionWorkflowConfiguration build() {
            ingestionWorkflowConfig.setContainerConfiguration("ingestionWorkflow",
                    EngineConstants.PRODATA_CUSTOMERSPACE, "IngestionWorkflow");
            ingestionWorkflowConfig
                    .setCustomerSpace(CustomerSpace.parse(PropDataConstants.SERVICE_CUSTOMERSPACE));
            ingestionWorkflowConfig.add(ingestionConfig);
            return ingestionWorkflowConfig;
        }
    }
}
