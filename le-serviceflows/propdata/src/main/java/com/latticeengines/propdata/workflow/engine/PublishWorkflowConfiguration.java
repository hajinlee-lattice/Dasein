package com.latticeengines.propdata.workflow.engine;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.propdata.manage.Publication;
import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.propdata.core.PropDataConstants;
import com.latticeengines.propdata.workflow.engine.steps.EngineConstants;
import com.latticeengines.propdata.workflow.engine.steps.PublishConfiguration;

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
            configuration.setCustomerSpace(CustomerSpace.parse(PropDataConstants.SERVICE_CUSTOMERSPACE));
            configuration.add(publishConfig);
            return configuration;
        }

    }

}
