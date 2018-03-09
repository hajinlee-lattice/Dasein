package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CombineStatisticsConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.ExportDataToRedshiftConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;

public class ProcessRatingWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private ProcessRatingWorkflowConfiguration configuration = new ProcessRatingWorkflowConfiguration();

        private ProcessRatingStepConfiguration processRatingStepConfiguration = new ProcessRatingStepConfiguration();
        private GenerateRatingWorkflowConfiguration.Builder generateRatingWorfklowConfigurationBuilder = new GenerateRatingWorkflowConfiguration.Builder();
        private CombineStatisticsConfiguration combineStatisticsConfiguration = new CombineStatisticsConfiguration();
        private ExportDataToRedshiftConfiguration exportDataToRedshiftConfiguration = new ExportDataToRedshiftConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processRatingStepConfiguration.setCustomerSpace(customerSpace);
            combineStatisticsConfiguration.setCustomerSpace(customerSpace);
            generateRatingWorfklowConfigurationBuilder.customer(customerSpace);
            exportDataToRedshiftConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            generateRatingWorfklowConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            exportDataToRedshiftConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            processRatingStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            exportDataToRedshiftConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder hdfsToRedshiftConfiguration(HdfsToRedshiftConfiguration hdfsToRedshiftConfiguration) {
            exportDataToRedshiftConfiguration.setHdfsToRedshiftConfiguration(hdfsToRedshiftConfiguration);
            return this;
        }

        public Builder dataCloudVersion(DataCloudVersion dataCloudVersion) {
            generateRatingWorfklowConfigurationBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchYarnQueue(String matchYarnQueue) {
            generateRatingWorfklowConfigurationBuilder.matchYarnQueue(matchYarnQueue);
            return this;
        }

        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            if (CollectionUtils.isNotEmpty(entities)) {
                if (entities.contains(BusinessEntity.Rating)) {
                    processRatingStepConfiguration.setRebuild(true);
                }
            }
            return this;
        }

        public ProcessRatingWorkflowConfiguration build() {
            configuration.setContainerConfiguration("processRatingWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());

            configuration.add(processRatingStepConfiguration);
            configuration.add(combineStatisticsConfiguration);
            configuration.add(generateRatingWorfklowConfigurationBuilder.build());
            configuration.add(exportDataToRedshiftConfiguration);
            return configuration;
        }
    }
}
