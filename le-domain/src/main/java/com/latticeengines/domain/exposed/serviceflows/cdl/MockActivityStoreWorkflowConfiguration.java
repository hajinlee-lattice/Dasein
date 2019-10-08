package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MockActivityStoreConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToDynamoStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToRedshiftStepConfiguration;

public class MockActivityStoreWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {

        private MockActivityStoreWorkflowConfiguration configuration = new MockActivityStoreWorkflowConfiguration();

        private MockActivityStoreConfiguration mock = new MockActivityStoreConfiguration();
        private ExportToRedshiftStepConfiguration exportToRedshift = new ExportToRedshiftStepConfiguration();
        private ExportToDynamoStepConfiguration exportToDynamo = new ExportToDynamoStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("mockActivityStoreWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            mock.setCustomerSpace(customerSpace);
            exportToDynamo.setCustomerSpace(customerSpace);
            exportToRedshift.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder dynamoSignature(String signature) {
            exportToDynamo.setDynamoSignature(signature);
            return this;
        }

        public MockActivityStoreWorkflowConfiguration build() {
            configuration.add(mock);
            configuration.add(exportToRedshift);
            configuration.add(exportToDynamo);
            return configuration;
        }

    }

}
