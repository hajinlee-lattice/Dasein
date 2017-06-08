package com.latticeengines.domain.exposed.serviceflows.datacloud.etl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.EngineConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.PublishConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class PublishWorkflowConfiguration extends WorkflowConfiguration {

    public static class Builder {

        private PublishWorkflowConfiguration configuration = new PublishWorkflowConfiguration();
        private PublishConfiguration publishConfig = new PublishConfiguration();

        public Builder hdfsPodId(String hdfsPodId) {
            publishConfig.setHdfsPodId(hdfsPodId);
            return this;
        }

        public Builder publication(Publication publication) {
            publishConfig.setPublication(publication);
            return this;
        }

        public Builder progress(PublicationProgress progress) {
            publishConfig.setProgress(progress);
            return this;
        }

        public Builder avroDir(String avroDir) {
            publishConfig.setAvroDir(avroDir);
            return this;
        }

        public PublishWorkflowConfiguration build() {
            configuration.setContainerConfiguration("publishWorkflow", EngineConstants.PRODATA_CUSTOMERSPACE,
                    "PublishWorkflow");
            configuration.setCustomerSpace(CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
            configuration.add(publishConfig);
            return configuration;
        }

    }

}
