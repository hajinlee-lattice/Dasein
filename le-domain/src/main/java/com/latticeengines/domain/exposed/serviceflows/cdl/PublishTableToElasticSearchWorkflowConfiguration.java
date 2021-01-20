package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.elasticsearch.ElasticSearchConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.publish.ImportPublishTableFromS3StepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.publish.PublishTableToElasticSearchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ElasticSearchExportConfig;

public class PublishTableToElasticSearchWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String WORKFLOW_NAME = "publishTableToElasticSearchWorkflow";
    public static class Builder {
        private final PublishTableToElasticSearchWorkflowConfiguration configuration =
                new PublishTableToElasticSearchWorkflowConfiguration();

        private final PublishTableToElasticSearchStepConfiguration publishTableToElasticSearchStepConfiguration =
                new PublishTableToElasticSearchStepConfiguration();

        private final ImportPublishTableFromS3StepConfiguration importPublishTableFromS3StepConfiguration =
                new ImportPublishTableFromS3StepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            publishTableToElasticSearchStepConfiguration.setCustomer(customerSpace.toString());
            importPublishTableFromS3StepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }


        public Builder signature(String signature) {
            publishTableToElasticSearchStepConfiguration.setSignature(signature);
            return this;
        }

        public Builder exportConfigs(List<ElasticSearchExportConfig> configs) {
            publishTableToElasticSearchStepConfiguration.setExportConfigs(configs);
            importPublishTableFromS3StepConfiguration.setExportConfigs(configs);
            return this;
        }

        public Builder esConfigs(ElasticSearchConfig esConfig) {
            publishTableToElasticSearchStepConfiguration.setEsConfigs(esConfig);
            return this;
        }

        public PublishTableToElasticSearchWorkflowConfiguration build() {
            configuration.setContainerConfiguration(WORKFLOW_NAME, configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(importPublishTableFromS3StepConfiguration);
            configuration.add(publishTableToElasticSearchStepConfiguration);
            return configuration;
        }
    }

}
