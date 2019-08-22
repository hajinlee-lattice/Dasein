package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CombineStatisticsConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessRatingStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToDynamoStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToRedshiftStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class ProcessRatingWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @JsonProperty("max_iteration")
    private int maxIteration;

    public int getMaxIteration() {
        return maxIteration;
    }

    public void setMaxIteration(int maxIteration) {
        this.maxIteration = maxIteration;
    }

    public static class Builder {
        private ProcessRatingWorkflowConfiguration configuration = new ProcessRatingWorkflowConfiguration();

        private ProcessRatingStepConfiguration processRatingStepConfiguration = new ProcessRatingStepConfiguration();
        private GenerateRatingWorkflowConfiguration.Builder generateRatingWorfklow = new GenerateRatingWorkflowConfiguration.Builder();
        private GenerateRatingStepConfiguration generateRatingStepConfiguration = new GenerateRatingStepConfiguration();
        private CombineStatisticsConfiguration combineStatisticsConfiguration = new CombineStatisticsConfiguration();
        private ExportToRedshiftStepConfiguration exportDataToRedshiftConfiguration = new ExportToRedshiftStepConfiguration();
        private ExportToDynamoStepConfiguration exportToDynamo = new ExportToDynamoStepConfiguration();
        private ImportExportS3StepConfiguration importExportS3 = new ImportExportS3StepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processRatingStepConfiguration.setCustomerSpace(customerSpace);
            combineStatisticsConfiguration.setCustomerSpace(customerSpace);
            generateRatingWorfklow.customer(customerSpace);
            exportDataToRedshiftConfiguration.setCustomerSpace(customerSpace);
            exportToDynamo.setCustomerSpace(customerSpace);
            generateRatingStepConfiguration.setCustomerSpace(customerSpace);
            importExportS3.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            generateRatingWorfklow.microServiceHostPort(microServiceHostPort);
            exportDataToRedshiftConfiguration.setMicroServiceHostPort(microServiceHostPort);
            exportToDynamo.setMicroServiceHostPort(microServiceHostPort);
            generateRatingStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            importExportS3.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            generateRatingStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processRatingStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            exportDataToRedshiftConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            exportToDynamo.setInternalResourceHostPort(internalResourceHostPort);
            generateRatingWorfklow.internalResourceHostPort(internalResourceHostPort);
            importExportS3.setInternalResourceHostPort(internalResourceHostPort);
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

        public Builder maxIteration(int maxIteraton) {
            configuration.setMaxIteration(maxIteraton);
            processRatingStepConfiguration.setMaxIteration(maxIteraton);
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup,
                List<TransformDefinition> stdTransformDefns) {
            generateRatingWorfklow.transformationGroup(transformationGroup, stdTransformDefns);
            return this;
        }

        public Builder dynamoSignature(String signature) {
            exportToDynamo.setDynamoSignature(signature);
            return this;
        }

        public Builder targetScoreDerivationEnabled(boolean targetScoreDerivationEnabled) {
            generateRatingWorfklow.targetScoreDerivationEnabled(targetScoreDerivationEnabled);
            return this;
        }

        public Builder apsRollupPeriod(String period) {
            generateRatingWorfklow.apsRollupPeriod(period);
            return this;
        }

        public ProcessRatingWorkflowConfiguration build() {
            generateRatingWorfklow.uniqueKeyColumn(InterfaceName.__Composite_Key__.name());
            generateRatingWorfklow.matchGroupId(InterfaceName.AccountId.name());
            generateRatingWorfklow.setUseScorederivation(false);
            generateRatingWorfklow.cdlMultiModel(true);
            generateRatingWorfklow.fetchOnly(true);
            generateRatingWorfklow.useAccountFeature(true);
            generateRatingWorfklow.exportKeyColumnsOnly(true);
            generateRatingWorfklow.forceEVSteps(true);

            configuration.setContainerConfiguration("processRatingWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());

            configuration.add(processRatingStepConfiguration);
            configuration.add(combineStatisticsConfiguration);
            configuration.add(generateRatingWorfklow.build());
            configuration.add(generateRatingStepConfiguration);
            configuration.add(exportDataToRedshiftConfiguration);
            configuration.add(exportToDynamo);
            configuration.add(importExportS3);
            return configuration;
        }
    }
}
