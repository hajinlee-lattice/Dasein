package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.PublishVIDataStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportTableRoleFromS3StepConfiguration;

public class PublishVIDataWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private PublishVIDataWorkflowConfiguration configuration = new PublishVIDataWorkflowConfiguration();
        private ImportTableRoleFromS3StepConfiguration importTableRoleFromS3StepConfiguration =
                new ImportTableRoleFromS3StepConfiguration();
        private PublishVIDataStepConfiguration publishVIDataStepConfiguration = new PublishVIDataStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            importTableRoleFromS3StepConfiguration.setCustomerSpace(customerSpace);
            publishVIDataStepConfiguration.setCustomer(customerSpace.toString());
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder version(DataCollection.Version version) {
            publishVIDataStepConfiguration.setVersion(version);
            return this;
        }

        public PublishVIDataWorkflowConfiguration build() {
            configuration.setContainerConfiguration("publishVIDataWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            importTableRoleFromS3StepConfiguration.setTableRoleInCollections(getTableRole());
            configuration.add(importTableRoleFromS3StepConfiguration);
            configuration.add(publishVIDataStepConfiguration);
            return configuration;
        }

        private List<TableRoleInCollection> getTableRole() {
            List<TableRoleInCollection> list = new ArrayList<>();
            list.add(TableRoleInCollection.ConsolidatedActivityStream);
            list.add(TableRoleInCollection.LatticeAccount);
            return list;
        }
    }
}
