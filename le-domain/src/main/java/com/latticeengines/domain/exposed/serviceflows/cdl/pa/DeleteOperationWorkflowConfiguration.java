package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Set;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.SoftDeleteAccountConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.SoftDeleteContactConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.SoftDeleteTransactionConfiguration;

public class DeleteOperationWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        DeleteOperationWorkflowConfiguration configuration = new DeleteOperationWorkflowConfiguration();

        SoftDeleteAccountConfiguration softDeleteAccountConfiguration = new SoftDeleteAccountConfiguration();
        SoftDeleteContactConfiguration softDeleteContactConfiguration = new SoftDeleteContactConfiguration();
        SoftDeleteTransactionConfiguration softDeleteTransactionConfiguration = new SoftDeleteTransactionConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            softDeleteAccountConfiguration.setCustomerSpace(customerSpace);
            softDeleteContactConfiguration.setCustomerSpace(customerSpace);
            softDeleteTransactionConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            softDeleteAccountConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            softDeleteContactConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            softDeleteTransactionConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder replaceEntities(Set<BusinessEntity> entities) {
            softDeleteAccountConfiguration.setNeedReplace(entities.contains(BusinessEntity.Account));
            softDeleteAccountConfiguration.setNeedReplace(entities.contains(BusinessEntity.Account));
            softDeleteAccountConfiguration.setNeedReplace(entities.contains(BusinessEntity.Account));
            return this;
        }

        public DeleteOperationWorkflowConfiguration build() {
            configuration.setContainerConfiguration("deleteOperationWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(softDeleteAccountConfiguration);
            configuration.add(softDeleteContactConfiguration);
            configuration.add(softDeleteTransactionConfiguration);
            return configuration;
        }
    }
}
