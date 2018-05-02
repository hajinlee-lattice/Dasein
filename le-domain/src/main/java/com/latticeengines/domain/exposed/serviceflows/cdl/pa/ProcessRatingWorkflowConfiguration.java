package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CombineStatisticsConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.ExportToRedshiftStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class ProcessRatingWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private ProcessRatingWorkflowConfiguration configuration = new ProcessRatingWorkflowConfiguration();

        private ProcessRatingStepConfiguration processRatingStepConfiguration = new ProcessRatingStepConfiguration();
        private GenerateRatingWorkflowConfiguration.Builder generateRatingWorfklow = new GenerateRatingWorkflowConfiguration.Builder();
        private GenerateRatingStepConfiguration generateRatingStepConfiguration = new GenerateRatingStepConfiguration();
        private CombineStatisticsConfiguration combineStatisticsConfiguration = new CombineStatisticsConfiguration();
        private ExportToRedshiftStepConfiguration exportDataToRedshiftConfiguration = new ExportToRedshiftStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processRatingStepConfiguration.setCustomerSpace(customerSpace);
            combineStatisticsConfiguration.setCustomerSpace(customerSpace);
            generateRatingWorfklow.customer(customerSpace);
            exportDataToRedshiftConfiguration.setCustomerSpace(customerSpace);
            generateRatingStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            generateRatingWorfklow.microServiceHostPort(microServiceHostPort);
            exportDataToRedshiftConfiguration.setMicroServiceHostPort(microServiceHostPort);
            generateRatingStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            processRatingStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            exportDataToRedshiftConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder dataCloudVersion(DataCloudVersion dataCloudVersion) {
            generateRatingWorfklow.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchYarnQueue(String matchYarnQueue) {
            generateRatingWorfklow.matchYarnQueue(matchYarnQueue);
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

        public Builder transformationGroup(TransformationGroup transformationGroup,
                List<TransformDefinition> stdTransformDefns) {
            generateRatingWorfklow.transformationGroup(transformationGroup, stdTransformDefns);
            return this;
        }

        public ProcessRatingWorkflowConfiguration build() {
            generateRatingWorfklow.uniqueKeyColumn(InterfaceName.__Composite_Key__.name());
            generateRatingWorfklow.setUseScorederivation(false);
            generateRatingWorfklow.cdlMultiModel(true);
            generateRatingWorfklow.fetchOnly(true);

            configuration.setContainerConfiguration("processRatingWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());

            configuration.add(processRatingStepConfiguration);
            configuration.add(combineStatisticsConfiguration);
            configuration.add(generateRatingWorfklow.build());
            configuration.add(generateRatingStepConfiguration);
            configuration.add(exportDataToRedshiftConfiguration);
            return configuration;
        }
    }
}
