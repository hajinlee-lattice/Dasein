package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.SetUtils;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CombineStatisticsConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ApsGenerationStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.TimeLineSparkStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.AtlasAccountLookupExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportTimelineRawTableToDynamoStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToDynamoStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToRedshiftStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.PublishToElasticSearchConfiguration;
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

    @SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
    public static class Builder {

        private ProcessAnalyzeWorkflowConfiguration configuration = new ProcessAnalyzeWorkflowConfiguration();
        private ProcessStepConfiguration processStepConfiguration = new ProcessStepConfiguration();

        private MatchEntityWorkflowConfiguration.Builder matchEntityWorkflowBuilder = new MatchEntityWorkflowConfiguration.Builder();
        private ProcessAccountWorkflowConfiguration.Builder processAccountWorkflowBuilder = new ProcessAccountWorkflowConfiguration.Builder();
        private ProcessContactWorkflowConfiguration.Builder processContactWorkflowBuilder = new ProcessContactWorkflowConfiguration.Builder();
        private ProcessProductWorkflowConfiguration.Builder processProductWorkflowBuilder = new ProcessProductWorkflowConfiguration.Builder();
        private ProcessTransactionWorkflowConfiguration.Builder processTransactionWorkflowBuilder = new ProcessTransactionWorkflowConfiguration.Builder();
        private ProcessCatalogWorkflowConfiguration.Builder processCatalogWorkflowBuilder = new ProcessCatalogWorkflowConfiguration.Builder();
        private ProcessActivityStreamWorkflowConfiguration.Builder processActivityStreamWorkflowBuilder = new ProcessActivityStreamWorkflowConfiguration.Builder();
        private CuratedAttributesWorkflowConfiguration.Builder curatedAttributesWorkflowBuilder = new CuratedAttributesWorkflowConfiguration.Builder();
        private ProcessRatingWorkflowConfiguration.Builder processRatingWorkflowBuilder = new ProcessRatingWorkflowConfiguration.Builder();
        private CommitEntityMatchWorkflowConfiguration.Builder commitEntityWorkflowBuilder = new CommitEntityMatchWorkflowConfiguration.Builder();

        private CombineStatisticsConfiguration combineStatisticsConfiguration = new CombineStatisticsConfiguration();
        private ExportToRedshiftStepConfiguration exportToRedshift = new ExportToRedshiftStepConfiguration();
        private ExportToDynamoStepConfiguration exportToDynamo = new ExportToDynamoStepConfiguration();
        private ExportTimelineRawTableToDynamoStepConfiguration exportTimelineRawTableToDynamo = new ExportTimelineRawTableToDynamoStepConfiguration();
        private TimeLineSparkStepConfiguration timeLineSparkStepConfiguration = new TimeLineSparkStepConfiguration();
        private AtlasAccountLookupExportStepConfiguration atlasAccountLookupExportStepConfiguration = new AtlasAccountLookupExportStepConfiguration();
        private AWSPythonBatchConfiguration awsPythonDataConfiguration = new AWSPythonBatchConfiguration();
        private ApsGenerationStepConfiguration apsGenerationStepConfiguration = new ApsGenerationStepConfiguration();
        private ImportExportS3StepConfiguration importExportS3 = new ImportExportS3StepConfiguration();
        private ConvertBatchStoreToDataTableWorkflowConfiguration.Builder convertBatchStoreToDataTableWorkflowBuilder = new ConvertBatchStoreToDataTableWorkflowConfiguration.Builder();
        private LegacyDeleteWorkflowConfiguration.Builder legacyDeleteWorkFlowBuilder = new LegacyDeleteWorkflowConfiguration.Builder();
        private PublishToElasticSearchConfiguration publishToElasticSearchConfiguration =
                new PublishToElasticSearchConfiguration();
        private GenerateVisitReportWorkflowConfiguration.Builder generateVisitReportWorkflowBuilder =
                new GenerateVisitReportWorkflowConfiguration.Builder();


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
            processCatalogWorkflowBuilder.customer(customerSpace);
            processActivityStreamWorkflowBuilder.customer(customerSpace);
            curatedAttributesWorkflowBuilder.customer(customerSpace);
            processRatingWorkflowBuilder.customer(customerSpace);
            commitEntityWorkflowBuilder.customer(customerSpace);
            convertBatchStoreToDataTableWorkflowBuilder.customer(customerSpace);
            legacyDeleteWorkFlowBuilder.customer(customerSpace);
            combineStatisticsConfiguration.setCustomerSpace(customerSpace);
            exportToRedshift.setCustomerSpace(customerSpace);
            exportToDynamo.setCustomerSpace(customerSpace);
            exportTimelineRawTableToDynamo.setCustomerSpace(customerSpace);
            timeLineSparkStepConfiguration.setCustomer(customerSpace.toString());
            atlasAccountLookupExportStepConfiguration.setCustomerSpace(customerSpace);
            awsPythonDataConfiguration.setCustomerSpace(customerSpace);
            apsGenerationStepConfiguration.setCustomer(customerSpace.getTenantId());
            importExportS3.setCustomerSpace(customerSpace);
            publishToElasticSearchConfiguration.setCustomerSpace(customerSpace);
            generateVisitReportWorkflowBuilder.customer(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            processStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            exportToRedshift.setMicroServiceHostPort(microServiceHostPort);
            exportToDynamo.setMicroServiceHostPort(microServiceHostPort);
            exportTimelineRawTableToDynamo.setMicroServiceHostPort(microServiceHostPort);
            atlasAccountLookupExportStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            processRatingWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            awsPythonDataConfiguration.setMicroServiceHostPort(microServiceHostPort);
            processTransactionWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            importExportS3.setMicroServiceHostPort(microServiceHostPort);
            publishToElasticSearchConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            processStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            matchEntityWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processAccountWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processContactWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processProductWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processTransactionWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processCatalogWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processActivityStreamWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            curatedAttributesWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            processRatingWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            commitEntityWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            convertBatchStoreToDataTableWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            legacyDeleteWorkFlowBuilder.internalResourceHostPort(internalResourceHostPort);
            exportToRedshift.setInternalResourceHostPort(internalResourceHostPort);
            exportToDynamo.setInternalResourceHostPort(internalResourceHostPort);
            exportTimelineRawTableToDynamo.setInternalResourceHostPort(internalResourceHostPort);
            atlasAccountLookupExportStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            awsPythonDataConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importExportS3.setInternalResourceHostPort(internalResourceHostPort);
            publishToElasticSearchConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            generateVisitReportWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
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
            processActivityStreamWorkflowBuilder.rebuildEntities(entities);
            if (SetUtils.emptyIfNull(entities).contains(BusinessEntity.ActivityStream)) {
                timeLineSparkStepConfiguration.setShouldRebuild(true);
                generateVisitReportWorkflowBuilder.setRebuildMode(true);
            }
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
            atlasAccountLookupExportStepConfiguration.setDynamoSignature(signature);
            return this;
        }

        public Builder timelineDynamoSignature(String timelineSignature) {
            exportTimelineRawTableToDynamo.setDynamoSignature(timelineSignature);
            processActivityStreamWorkflowBuilder.timelineDynamoSignature(timelineSignature);
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
            processRatingWorkflowBuilder.apsRollupPeriod(apsRollingPeriod);
            processTransactionWorkflowBuilder.apsRollingPeriod(apsRollingPeriod);
            return this;
        }

        public Builder apsImputationEnabled(boolean apsImputationEnabled) {
            apsGenerationStepConfiguration.setApsImputationEnabled(apsImputationEnabled);
            return this;
        }

        public Builder targetScoreDerivationEnabled(boolean targetScoreDerivationEnabled) {
            processRatingWorkflowBuilder.targetScoreDerivationEnabled(targetScoreDerivationEnabled);
            processStepConfiguration.setTargetScoreDerivation(targetScoreDerivationEnabled);
            return this;
        }

        public Builder allowInternalEnrichAttrs(boolean allowInternalEnrichAttrs) {
            processAccountWorkflowBuilder.allowInternalEnrichAttrs(allowInternalEnrichAttrs);
            return this;
        }

        public Builder catalogTables(Map<String, String> catalogTables) {
            processCatalogWorkflowBuilder.catalogTables(catalogTables);
            return this;
        }

        public Builder catalogIngestionBehaivors(Map<String, DataFeedTask.IngestionBehavior> ingestionBehaviors) {
            processCatalogWorkflowBuilder.catalogIngestionBehaviors(ingestionBehaviors);
            return this;
        }

        public Builder catalogPrimaryKeyColumns(Map<String, String> primaryKeyColumns) {
            processCatalogWorkflowBuilder.catalogPrimaryKeyColumns(primaryKeyColumns);
            return this;
        }

        public Builder catalogImports(Map<String, List<ActivityImport>> catalogImports) {
            processCatalogWorkflowBuilder.catalogImports(catalogImports);
            generateVisitReportWorkflowBuilder.setCatalogImports(catalogImports);
            return this;
        }

        public Builder activeRawStreamTables(Map<String, String> rawStreamTables) {
            processActivityStreamWorkflowBuilder.activeRawStreamTables(rawStreamTables);
            convertBatchStoreToDataTableWorkflowBuilder.activeRawStreamTables(rawStreamTables);
            matchEntityWorkflowBuilder.activeRawStreamTables(rawStreamTables);
            return this;
        }

        public Builder activityStreams(Map<String, AtlasStream> streams) {
            processActivityStreamWorkflowBuilder.activityStreams(streams);
            convertBatchStoreToDataTableWorkflowBuilder.activityStreams(streams);
            matchEntityWorkflowBuilder.activityStreams(streams);
            generateVisitReportWorkflowBuilder.activityStreams(streams);
            return this;
        }

        public Builder activityMetricsGroups(Map<String, ActivityMetricsGroup> groups) {
            processActivityStreamWorkflowBuilder.activityMetricsGroups(groups);
            convertBatchStoreToDataTableWorkflowBuilder.activityMetricsGroups(groups);
            return this;
        }

        public Builder activityStreamImports(Map<String, List<ActivityImport>> activityStreamImports) {
            processActivityStreamWorkflowBuilder.activityStreamImports(activityStreamImports);
            convertBatchStoreToDataTableWorkflowBuilder.activityStreamImports(activityStreamImports);
            matchEntityWorkflowBuilder.activityStreamImports(activityStreamImports);
            generateVisitReportWorkflowBuilder.activityStreamImports(activityStreamImports);
            return this;
        }

        public Builder systemIdMap(Map<String, List<String>> systemIds) {
            matchEntityWorkflowBuilder.systemIdMap(systemIds);
            processAccountWorkflowBuilder.systemIdMap(systemIds);
            processContactWorkflowBuilder.systemIdMap(systemIds);
            return this;
        }

        public Builder defaultSystemIdMap(Map<String, String> defaultSystemIds) {
            matchEntityWorkflowBuilder.defaultSystemIdMap(defaultSystemIds);
            return this;
        }

        public Builder entityMatchEnabled(boolean entityMatchEnabled) {
            processStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            matchEntityWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            processAccountWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            processContactWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            processTransactionWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            processCatalogWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            processActivityStreamWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            convertBatchStoreToDataTableWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            commitEntityWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            curatedAttributesWorkflowBuilder.entityMatchEnabled(entityMatchEnabled);
            return this;
        }

        public Builder entityMatchGAOnly(boolean gaOnly) {
            matchEntityWorkflowBuilder.entityMatchGAOnly(gaOnly);
            processActivityStreamWorkflowBuilder.entityMatchGAOnly(gaOnly);
            convertBatchStoreToDataTableWorkflowBuilder.entityMatchGAOnly(gaOnly);
            legacyDeleteWorkFlowBuilder.entityMatchGAOnly(gaOnly);
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
            exportTimelineRawTableToDynamo.setSkipStep(skipDynamoExport);
            // atlasAccountLookupExportStepConfiguration.setSkipStep(skipDynamoExport);
            return this;
        }

        public Builder fullRematch(boolean fullRematch) {
            processStepConfiguration.setFullRematch(fullRematch);
            processActivityStreamWorkflowBuilder.setRematchMode(fullRematch);
            convertBatchStoreToDataTableWorkflowBuilder.setRematchMode(fullRematch);
            matchEntityWorkflowBuilder.setRematchMode(fullRematch);
            generateVisitReportWorkflowBuilder.setRematchMode(fullRematch);
            return this;
        }

        public Builder entityMatchConfiguration(EntityMatchConfiguration configuration) {
            processStepConfiguration.setEntityMatchConfiguration(configuration);
            processActivityStreamWorkflowBuilder.entityMatchConfiguration(configuration);
            matchEntityWorkflowBuilder.entityMatchConfiguration(configuration);
            return this;
        }

        public Builder autoSchedule(boolean autoSchedule) {
            processStepConfiguration.setAutoSchedule(autoSchedule);
            return this;
        }

        public Builder fullProfile(boolean fullProfile) {
            processAccountWorkflowBuilder.fullProfile(fullProfile);
            return this;
        }

        public Builder skipPublishToS3(boolean skip) {
            processStepConfiguration.setSkipPublishToS3(skip);
            return this;
        }

        public Builder replaceEntities(Set<BusinessEntity> entities) {
            processAccountWorkflowBuilder.setReplace(entities.contains(BusinessEntity.Account));
            processContactWorkflowBuilder.setReplace(entities.contains(BusinessEntity.Contact));
            processProductWorkflowBuilder.setReplace(entities.contains(BusinessEntity.Product));
            processActivityStreamWorkflowBuilder.setReplaceMode(entities.contains(BusinessEntity.ActivityStream));
            convertBatchStoreToDataTableWorkflowBuilder
                    .setReplaceMode(entities.contains(BusinessEntity.ActivityStream));
            matchEntityWorkflowBuilder.setReplaceMode(entities.contains(BusinessEntity.ActivityStream));
            processTransactionWorkflowBuilder.setReplace(entities.contains(BusinessEntity.Transaction));
            generateVisitReportWorkflowBuilder.setReplaceMode(entities.contains(BusinessEntity.ActivityStream));
            return this;
        }

        public Builder skipEntityMatchRematch(Set<BusinessEntity> entities) {
            convertBatchStoreToDataTableWorkflowBuilder.setSkipStep(entities);
            return this;
        }

        public Builder setConvertServiceConfig(HashMap<TableRoleInCollection, Table> batchStoresToConvert) {
            convertBatchStoreToDataTableWorkflowBuilder.setConvertServiceConfig(batchStoresToConvert);
            return this;
        }

        public Builder activeTimelineList(List<TimeLine> timeLineList) {
            processActivityStreamWorkflowBuilder.activeTimelineList(timeLineList);
            return this;
        }

        public Builder templateToSystemTypeMap(Map<String, S3ImportSystem.SystemType> templateToSystemTypeMap) {
            processActivityStreamWorkflowBuilder.templateToSystemTypeMap(templateToSystemTypeMap);
            return this;
        }

        public Builder eraseByNullEnabled(boolean eraseByNullEnabled) {
            processAccountWorkflowBuilder.eraseByNullEnabled(eraseByNullEnabled);
            processContactWorkflowBuilder.eraseByNullEnabled(eraseByNullEnabled);
            return this;
        }

        public Builder isSSVITenant(boolean isSSVITenant) {
            processStepConfiguration.setSSVITenant(isSSVITenant);
            return this;
        }

        public Builder isCDLTenant(boolean isCDLTenant) {
            processStepConfiguration.setCDLTenant(isCDLTenant);
            return this;
        }

        public Builder setCatalog(List<Catalog> catalogs) {
            generateVisitReportWorkflowBuilder.setCatalog(catalogs);
            return this;
        }

        public ProcessAnalyzeWorkflowConfiguration build() {
            configuration.setContainerConfiguration("processAnalyzeWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(processStepConfiguration);
            configuration.add(convertBatchStoreToDataTableWorkflowBuilder.build());
            configuration.add(legacyDeleteWorkFlowBuilder.build());
            configuration.add(matchEntityWorkflowBuilder.build());
            configuration.add(processAccountWorkflowBuilder.build());
            configuration.add(processContactWorkflowBuilder.build());
            configuration.add(processProductWorkflowBuilder.build());
            configuration.add(processTransactionWorkflowBuilder.build());
            configuration.add(processCatalogWorkflowBuilder.build());
            configuration.add(processActivityStreamWorkflowBuilder.build());
            configuration.add(curatedAttributesWorkflowBuilder.build());
            configuration.add(processRatingWorkflowBuilder.build());
            configuration.add(commitEntityWorkflowBuilder.build());
            configuration.add(combineStatisticsConfiguration);
            configuration.add(exportToRedshift);
            configuration.add(exportToDynamo);
            configuration.add(exportTimelineRawTableToDynamo);
            configuration.add(timeLineSparkStepConfiguration);
            configuration.add(atlasAccountLookupExportStepConfiguration);
            configuration.add(awsPythonDataConfiguration);
            configuration.add(apsGenerationStepConfiguration);
            configuration.add(importExportS3);
            configuration.add(generateVisitReportWorkflowBuilder.build());
            configuration.add(publishToElasticSearchConfiguration);
            return configuration;
        }
    }
}
