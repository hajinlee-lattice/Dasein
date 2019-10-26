package com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch;

import java.util.HashMap;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreStepConfiguration;

public class ConvertTransactionWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private ConvertTransactionWorkflowConfiguration configuration =
                new ConvertTransactionWorkflowConfiguration();
        private ConvertBatchStoreStepConfiguration convertBatchStoreStepConfiguration =
                new ConvertBatchStoreStepConfiguration();
        private DeleteByUploadStepConfiguration deleteByUploadStepConfiguration = new DeleteByUploadStepConfiguration();

        public ConvertTransactionWorkflowConfiguration.Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            convertBatchStoreStepConfiguration.setCustomerSpace(customerSpace);
            deleteByUploadStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            convertBatchStoreStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            deleteByUploadStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder setSkipStep(boolean skipStep) {
            convertBatchStoreStepConfiguration.setSkipStep(skipStep);
            deleteByUploadStepConfiguration.setSkipStep(skipStep);
            return this;
        }

        public Builder setConvertServiceConfig(HashMap<TableRoleInCollection, Table> batchStoresToConvert) {
            RematchConvertServiceConfiguration serviceConfiguration = new RematchConvertServiceConfiguration();
            serviceConfiguration.setEntity(BusinessEntity.Transaction);
            serviceConfiguration.setBatchstoresToConvert(batchStoresToConvert);
            convertBatchStoreStepConfiguration.setConvertServiceConfig(serviceConfiguration);
            return this;
        }

        public Builder setNeedHardDeleteAccountSet(Set<String> hardDeleteAccountSet) {
            if (CollectionUtils.isEmpty(hardDeleteAccountSet)) {
                deleteByUploadStepConfiguration.setSkipStep(true);
            }
            deleteByUploadStepConfiguration.setHardDeleteTableSet(hardDeleteAccountSet);
            return this;
        }

        public ConvertTransactionWorkflowConfiguration build() {
            configuration.setContainerConfiguration("convertTransactionWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            convertBatchStoreStepConfiguration.setEntity(BusinessEntity.Transaction);
            configuration.add(convertBatchStoreStepConfiguration);
            configuration.add(deleteByUploadStepConfiguration);
            return configuration;
        }
    }
}
