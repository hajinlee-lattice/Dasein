package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CombineStatisticsConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ApsGenerationStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToDynamoStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToRedshiftStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSPythonBatchConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.CommitEntityMatchWorkflowConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class ProcessAnalyzeWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String WORKFLOW_NAME = "processAnalyzeWorkflow";

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

        private MatchEntityWorkflowConfiguration.Builder matchEntityWorkflowBuilder = new MatchEntityWorkflowConfiguration.Builder();
        private ProcessAccountWorkflowConfiguration.Builder processAccountWorkflowBuilder = new ProcessAccountWorkflowConfiguration.Builder();
        private ProcessContactWorkflowConfiguration.Builder processContactWorkflowBuilder = new ProcessContactWorkflowConfiguration.Builder();
        private ProcessProductWorkflowConfiguration.Builder processProductWorkflowBuilder = new ProcessProductWorkflowConfiguration.Builder();
        private ProcessTransactionWorkflowConfiguration.Builder processTransactionWorkflowBuilder = new ProcessTransactionWorkflowConfiguration.Builder();
        private CuratedAttributesWorkflowConfiguration.Builder curatedAttributesWorkflowBuilder = new CuratedAttributesWorkflowConfiguration.Builder();
        private ProcessRatingWorkflowConfiguration.Builder processRatingWorkflowBuilder = new ProcessRatingWorkflowConfiguration.Builder();
        private CommitEntityMatchWorkflowConfiguration.Builder commitEntityWorkflowBuilder = new CommitEntityMatchWorkflowConfiguration.Builder();

        private CombineStatisticsConfiguration combineStatisticsConfiguration = new CombineStatisticsConfiguration();
        private ExportToRedshiftStepConfiguration exportToRedshift = new ExportToRedshiftStepConfiguration();
        private ExportToDynamoStepConfiguration exportToDynamo = new ExportToDynamoStepConfiguration();
        private AWSPythonBatchConfiguration awsPythonDataConfiguration = new AWSPythonBatchConfiguration();
        private ApsGenerationStepConfiguration apsGenerationStepConfiguration = new ApsGenerationStepConfiguration();
        private ImportExportS3StepConfiguration importExportS3 = new ImportExportS3StepConfiguration();

        public Builder initialDataFeedStatus(DataFeed.Status initialDataFeedStatus) {
            processStepConfiguration.setInitialDataFeedStatus(initialDataFeedStatus);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processStepConfiguration.setCustomerSpace(customerSpace);
            matchEntityWorkflowBuilder.customer(customerSpace);
            processAccountWorkflowBuilder.customer(customerSpace);
            processContactWorkflowBuilder.customer(customerSpace);
            processProductWorkflowBuilder.customer(customerSpace);
            processTransactionWorkflowBuilder.customer(customerSpace);
            curatedAttributesWorkflowBuilder.customer(customerSpace);
            processRatingWorkflowBuilder.customer(customerSpace);
            commitEntityWorkflowBuilder.customer(customerSpace);
            combineStatisticsConfiguration.setCustomerSpace(customerSpace);
            exportToRedshift.setCustomerSpace(customerSpace);
            exportToDynamo.setCustomerSpace(customerSpace);
            awsPythonDataConfiguration.setCustomerSpace(customerSpace);
            apsGenerationStepConfiguration.setCustomer(customerSpace.getTenantId());
            importExportS3.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            processStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            exportToRedshift.setMicroServiceHostPort(microServiceHostPort);
            exportToDynamo.setMicroServiceHostPort(microServiceHostPort);
            processRatingWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            awsPythonDataConfiguration.setMicroServiceHostPort(microServiceHostPort);
            processTransactionWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            curatedAttributesWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            importExportS3.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            processStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            matchEntityWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processAccountWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processContactWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processProductWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processTransactionWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            curatedAttributesWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processRatingWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            commitEntityWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            exportToRedshift.setInternalResourceHostPort(internalResourceHostPort);
            exportToDynamo.setInternalResourceHostPort(internalResourceHostPort);
            awsPythonDataConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importExportS3.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            processStepConfiguration.setInputProperties(inputProperties);
            return this;
        }

        public Builder workflowContainerMem(int mb) {
            configuration.setContainerMemoryMB(mb);
            return this;
        }

        public Builder actionIds(List<Long> actionIds) {
            processStepConfiguration.setActionIds(actionIds);
            processTransactionWorkflowBuilder.actionIds(actionIds);
            return this;
        }

        public Builder ownerId(long ownerId) {
            processStepConfiguration.setOwnerId(ownerId);
            return this;
        }

        public Builder currentDataCloudBuildNumber(String currentDataCloudBuildNumber) {
            processStepConfiguration.setDataCloudBuildNumber(currentDataCloudBuildNumber);
            return this;
        }

        public Builder dataCloudVersion(DataCloudVersion dataCloudVersion) {
            processRatingWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            matchEntityWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            processAccountWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchYarnQueue(String matchYarnQueue) {
            processRatingWorkflowBuilder.matchYarnQueue(matchYarnQueue);
            return this;
        }

        public Builder rebuildEntities(Set<BusinessEntity> entities) {
            processStepConfiguration.setRebuildEntities(entities);
            processAccountWorkflowBuilder.rebuildEntities(entities);
            processContactWorkflowBuilder.rebuildEntities(entities);
            processProductWorkflowBuilder.rebuildEntities(entities);
            processTransactionWorkflowBuilder.rebuildEntities(entities);
            curatedAttributesWorkflowBuilder.rebuildEntities(entities);
            processRatingWorkflowBuilder.rebuildEntities(entities);
            return this;
        }

        public Builder rebuildSteps(List<String> steps) {
            HashSet<String> rebuildSteps = new HashSet<>();
            if (CollectionUtils.isNotEmpty(steps)) {
                steps.forEach(s -> rebuildSteps.add(s.toLowerCase()));
            }
            awsPythonDataConfiguration.setRebuildSteps(rebuildSteps);
            if (rebuildSteps.contains("apsgeneration")) {
                apsGenerationStepConfiguration.setForceRebuild(true);
            }
            return this;
        }

        public Builder ignoreDataCloudChange(Boolean ignoreDataCloudChange) {
            processStepConfiguration.setIgnoreDataCloudChange(ignoreDataCloudChange);
            return this;
        }

        public Builder userId(String userId) {
            configuration.setUserId(userId);
            processStepConfiguration.setUserId(userId);
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup,
                List<TransformDefinition> stdTransformDefns) {
            processRatingWorkflowBuilder.transformationGroup(transformationGroup, stdTransformDefns);
            return this;
        }

        public Builder dynamoSignature(String signature) {
            exportToDynamo.setDynamoSignature(signature);
            processRatingWorkflowBuilder.dynamoSignature(signature);
            return this;
        }

        public Builder maxRatingIteration(int maxRatingIteration) {
            processRatingWorkflowBuilder.maxIteration(maxRatingIteration);
            return this;
        }

        public Builder apsRollingPeriod(String apsRollingPeriod) {
            awsPythonDataConfiguration.setRollingPeriod(apsRollingPeriod);
            apsGenerationStepConfiguration.setRollingPeriod(apsRollingPeriod);
            processStepConfiguration.setApsRollingPeriod(apsRollingPeriod);
            return this;
        }

        public Builder entityMatchEnabled(boolean entityMatchEnabled) {
            processStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            matchEntityWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            processAccountWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            processContactWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            commitEntityWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            return this;
        }

        public Builder skipEntities(Set<BusinessEntity> entities) {
            processStepConfiguration.setSkipEntities(entities);
            if (CollectionUtils.containsAny(entities, BusinessEntity.AnalyticPurchaseState)) {
                awsPythonDataConfiguration.setSkipStep(true);
                apsGenerationStepConfiguration.setSkipStep(true);
            }
            return this;
        }

        public Builder skipDynamoExport(boolean skipDynamoExport) {
            exportToDynamo.setSkipStep(skipDynamoExport);
            return this;
        }

        public Builder fullRematch(boolean fullRematch) {
            processStepConfiguration.setFullRematch(fullRematch);
            return this;
        }

        public Builder autoSchedule(boolean autoSchedule) {
            processStepConfiguration.setAutoSchedule(autoSchedule);
            return this;
        }

        public Builder skipPublishToS3(boolean skip) {
            processStepConfiguration.setSkipPublishToS3(skip);
            return this;
        }

        public Builder dataQuotaLimit(Long dataQuotaLimit, BusinessEntity businessEntity) {
            switch (businessEntity) {
                case Account: processAccountWorkflowBuilder.dataQuotaLimit(dataQuotaLimit);break;
                case Contact: processContactWorkflowBuilder.dataQuotaLimit(dataQuotaLimit);break;
                case Transaction: processTransactionWorkflowBuilder.dataQuotaLimit(dataQuotaLimit);break;
                default:break;
            }
            return this;
        }

        public Builder dataQuotaLimit(Long dataQuotaLimit, ProductType type) {
            processProductWorkflowBuilder.dataQuotaLimit(type, dataQuotaLimit);
            return this;
        }

        public ProcessAnalyzeWorkflowConfiguration build() {
            configuration.setContainerConfiguration("processAnalyzeWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(processStepConfiguration);
            configuration.add(matchEntityWorkflowBuilder.build());
            configuration.add(processAccountWorkflowBuilder.build());
            configuration.add(processContactWorkflowBuilder.build());
            configuration.add(processProductWorkflowBuilder.build());
            configuration.add(processTransactionWorkflowBuilder.build());
            configuration.add(curatedAttributesWorkflowBuilder.build());
            configuration.add(processRatingWorkflowBuilder.build());
            configuration.add(commitEntityWorkflowBuilder.build());
            configuration.add(combineStatisticsConfiguration);
            configuration.add(exportToRedshift);
            configuration.add(exportToDynamo);
            configuration.add(awsPythonDataConfiguration);
            configuration.add(apsGenerationStepConfiguration);
            configuration.add(importExportS3);
            return configuration;
        }
    }
}
