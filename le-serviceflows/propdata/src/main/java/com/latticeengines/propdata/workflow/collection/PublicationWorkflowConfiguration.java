package com.latticeengines.propdata.workflow.collection;

import com.latticeengines.domain.exposed.propdata.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.propdata.workflow.collection.steps.CollectionWorkflowConstants;
import com.latticeengines.propdata.workflow.collection.steps.PublishConfiguration;

public class PublicationWorkflowConfiguration extends WorkflowConfiguration {

    public static class Builder {

        private PublicationWorkflowConfiguration configuration = new PublicationWorkflowConfiguration();
        private PublishConfiguration publishConfig = new PublishConfiguration();

        public Builder hdfsPodId(String hdfsPodId) {
            publishConfig.setHdfsPodId(hdfsPodId);
            return this;
        }

        public Builder publicationConfig(PublicationConfiguration publicationConfig) {
            publishConfig.setPublicationConfiguration(publicationConfig);
            return this;
        }

        public PublicationWorkflowConfiguration build() {
            configuration.setContainerConfiguration("publicationWorkflow", CollectionWorkflowConstants.PRODATA_CUSTOMERSPACE,
                    "PublicationWorkflow");
            configuration.add(publishConfig);
            return configuration;
        }

    }

}
