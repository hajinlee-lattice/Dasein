package com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch;

import java.util.HashMap;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreStepConfiguration;

public class ConvertAccountWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private ConvertAccountWorkflowConfiguration configuration =
                new ConvertAccountWorkflowConfiguration();
        private ConvertBatchStoreStepConfiguration convertBatchStoreStepConfiguration =
                new ConvertBatchStoreStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            convertBatchStoreStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            convertBatchStoreStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder setSkipStep(boolean skipStep) {
            convertBatchStoreStepConfiguration.setSkipStep(skipStep);
            return this;
        }

        public Builder setConvertServiceConfig(HashMap<TableRoleInCollection, Table> needConvertBatchStoreTables) {
            RematchConvertServiceConfiguration serviceConfiguration = new RematchConvertServiceConfiguration();
            serviceConfiguration.setEntity(BusinessEntity.Account);
            serviceConfiguration.setNeedConvertBatchStoreTables(needConvertBatchStoreTables);
            convertBatchStoreStepConfiguration.setConvertServiceConfig(serviceConfiguration);
            return this;
        }

        public ConvertAccountWorkflowConfiguration build() {
            configuration.setContainerConfiguration("convertAccountWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            convertBatchStoreStepConfiguration.setEntity(BusinessEntity.Account);
            configuration.add(convertBatchStoreStepConfiguration);
            return configuration;
        }
    }
}
