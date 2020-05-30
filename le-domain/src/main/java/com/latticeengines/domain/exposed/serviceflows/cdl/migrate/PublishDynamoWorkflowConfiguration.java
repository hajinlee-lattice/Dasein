package com.latticeengines.domain.exposed.serviceflows.cdl.migrate;

import java.util.ArrayList;
import java.util.Collection;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.PublishTableRoleStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToDynamoStepConfiguration;

public class PublishDynamoWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private PublishDynamoWorkflowConfiguration configuration = new PublishDynamoWorkflowConfiguration();

        private PublishTableRoleStepConfiguration step = new PublishTableRoleStepConfiguration();
        private ExportToDynamoStepConfiguration exportToDynamo = new ExportToDynamoStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("publishDynamoWorkflow", customerSpace, //
                    configuration.getClass().getSimpleName());
            step.setCustomerSpace(customerSpace);
            exportToDynamo.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder tableRoles(Collection<TableRoleInCollection> tableRoles) {
            step.setTableRoles(new ArrayList<>(tableRoles));
            return this;
        }

        public Builder version(DataCollection.Version version) {
            step.setVersion(version);
            return this;
        }

        public Builder dynamoSignature(String signature) {
            exportToDynamo.setDynamoSignature(signature);
            return this;
        }

        public PublishDynamoWorkflowConfiguration build() {
            configuration.add(step);
            configuration.add(exportToDynamo);
            return configuration;
        }
    }

}
