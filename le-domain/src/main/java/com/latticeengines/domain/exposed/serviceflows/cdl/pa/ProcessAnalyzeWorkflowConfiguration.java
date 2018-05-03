package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CombineStatisticsConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToRedshiftStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToDynamoStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSPythonBatchConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class ProcessAnalyzeWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Scoring.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {

        private ProcessAnalyzeWorkflowConfiguration configuration = new ProcessAnalyzeWorkflowConfiguration();
        private ProcessStepConfiguration processStepConfiguration = new ProcessStepConfiguration();

        private ProcessAccountWorkflowConfiguration.Builder processAccountWorkflowBuilder = new ProcessAccountWorkflowConfiguration.Builder();
        private ProcessContactWorkflowConfiguration.Builder processContactWorkflowBuilder = new ProcessContactWorkflowConfiguration.Builder();
        private ProcessProductWorkflowConfiguration.Builder processProductWorkflowBuilder = new ProcessProductWorkflowConfiguration.Builder();
        private ProcessTransactionWorkflowConfiguration.Builder processTransactionWorkflowBuilder = new ProcessTransactionWorkflowConfiguration.Builder();
        private ProcessRatingWorkflowConfiguration.Builder processRatingWorkflowBuilder = new ProcessRatingWorkflowConfiguration.Builder();

        private CombineStatisticsConfiguration combineStatisticsConfiguration = new CombineStatisticsConfiguration();
        private ExportToRedshiftStepConfiguration exportToRedshift = new ExportToRedshiftStepConfiguration();
        private ExportToDynamoStepConfiguration exportToDynamo = new ExportToDynamoStepConfiguration();
        private AWSPythonBatchConfiguration awsPythonDataConfiguration = new AWSPythonBatchConfiguration();

        public Builder initialDataFeedStatus(DataFeed.Status initialDataFeedStatus) {
            processStepConfiguration.setInitialDataFeedStatus(initialDataFeedStatus);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processStepConfiguration.setCustomerSpace(customerSpace);
            processAccountWorkflowBuilder.customer(customerSpace);
            processContactWorkflowBuilder.customer(customerSpace);
            processProductWorkflowBuilder.customer(customerSpace);
            processTransactionWorkflowBuilder.customer(customerSpace);
            processRatingWorkflowBuilder.customer(customerSpace);
            combineStatisticsConfiguration.setCustomerSpace(customerSpace);
            exportToRedshift.setCustomerSpace(customerSpace);
            exportToDynamo.setCustomerSpace(customerSpace);
            awsPythonDataConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            processStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            exportToRedshift.setMicroServiceHostPort(microServiceHostPort);
            exportToDynamo.setMicroServiceHostPort(microServiceHostPort);
            processRatingWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            awsPythonDataConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            processStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processAccountWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processContactWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processProductWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processTransactionWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processRatingWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            exportToRedshift.setInternalResourceHostPort(internalResourceHostPort);
            exportToDynamo.setInternalResourceHostPort(internalResourceHostPort);
            awsPythonDataConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder workflowContainerMem(int mb) {
            configuration.setContainerMemoryMB(mb);
            return this;
        }

        public Builder importAndDeleteJobIds(List<Long> importJobIds) {
            processStepConfiguration.setImportAndDeleteJobIds(importJobIds);
            return this;
        }

        public Builder actionIds(List<Long> actionIds) {
            processStepConfiguration.setActionIds(actionIds);
            return this;
        }

        public Builder currentDataCloudBuildNumber(String currentDataCloudBuildNumber) {
            processStepConfiguration.setDataCloudBuildNumber(currentDataCloudBuildNumber);
            return this;
        }

        public Builder dataCloudVersion(DataCloudVersion dataCloudVersion) {
            processRatingWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            processAccountWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchYarnQueue(String matchYarnQueue) {
            processRatingWorkflowBuilder.matchYarnQueue(matchYarnQueue);
            return this;
        }

        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            processAccountWorkflowBuilder.rebuildEntities(entities);
            processContactWorkflowBuilder.rebuildEntities(entities);
            processProductWorkflowBuilder.rebuildEntities(entities);
            processTransactionWorkflowBuilder.rebuildEntities(entities);
            processRatingWorkflowBuilder.rebuildEntities(entities);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup,
                                           List<TransformDefinition> stdTransformDefns) {
            processRatingWorkflowBuilder.transformationGroup(transformationGroup, stdTransformDefns);
            return this;
        }

        public Builder dynamoSignature(String signature) {
            exportToDynamo.setDynamoSignature(signature);
            return this;
        }

        public ProcessAnalyzeWorkflowConfiguration build() {
            configuration.setContainerConfiguration("processAnalyzeWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(processStepConfiguration);
            configuration.add(processAccountWorkflowBuilder.build());
            configuration.add(processContactWorkflowBuilder.build());
            configuration.add(processProductWorkflowBuilder.build());
            configuration.add(processTransactionWorkflowBuilder.build());
            configuration.add(processRatingWorkflowBuilder.build());
            configuration.add(combineStatisticsConfiguration);
            configuration.add(exportToRedshift);
            configuration.add(exportToDynamo);
            configuration.add(awsPythonDataConfiguration);
            return configuration;
        }
    }
}
