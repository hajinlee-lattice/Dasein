package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;

public class UpdateAccountWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private UpdateAccountWorkflowConfiguration configuration = new UpdateAccountWorkflowConfiguration();
        private ProcessAccountStepConfiguration processAccountStepConfiguration = new ProcessAccountStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processAccountStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            processAccountStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder dataCloudVersion(DataCloudVersion dataCloudVersion) {
            processAccountStepConfiguration.setDataCloudVersion(dataCloudVersion.getVersion());
            return this;
        }

        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            if (CollectionUtils.isNotEmpty(entities)) {
                if (entities.contains(BusinessEntity.Account)) {
                    processAccountStepConfiguration.setRebuild(true);
                }
            }
            return this;
        }

        public Builder setCleanup(Boolean needCleanup) {
            processAccountStepConfiguration.setNeedCleanup(needCleanup);
            return this;
        }

        public UpdateAccountWorkflowConfiguration build() {
            configuration.setContainerConfiguration("updateAccountWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());

            configuration.add(processAccountStepConfiguration);
            return configuration;
        }
    }
}
