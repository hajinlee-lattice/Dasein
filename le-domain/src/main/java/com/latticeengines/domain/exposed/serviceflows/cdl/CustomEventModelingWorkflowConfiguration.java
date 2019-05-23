package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.GenerateAIRatingWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.LdcOnlyAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.AddStandardAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseReportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.AttributeCategoryModifierConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.MergeUserRefinedAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.UseConfiguredModelingAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.ModelDataValidationWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.ModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.DedupEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ExportBucketToolStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ExportScoreTrainingFileStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.SetConfigurationForScoringConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class CustomEventModelingWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Modeling.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {
        private CustomEventModelingWorkflowConfiguration configuration = new CustomEventModelingWorkflowConfiguration();

        private ImportStepConfiguration importData = new ImportStepConfiguration();
        private BaseReportStepConfiguration registerReport = new BaseReportStepConfiguration();
        private ModelDataValidationWorkflowConfiguration.Builder modelDataValidationWorkflow = new ModelDataValidationWorkflowConfiguration.Builder();

        private CustomEventMatchWorkflowConfiguration.Builder customEventMatchWorkflowConfigurationBuilder = new CustomEventMatchWorkflowConfiguration.Builder();

        private DedupEventTableConfiguration dedupEventTable = new DedupEventTableConfiguration();
        private AddStandardAttributesConfiguration addStandardAttributes = new AddStandardAttributesConfiguration();

        private ModelWorkflowConfiguration.Builder modelWorkflowBuilder = new ModelWorkflowConfiguration.Builder();
        private ExportBucketToolStepConfiguration exportBucketTool = new ExportBucketToolStepConfiguration();
        private ExportScoreTrainingFileStepConfiguration exportScoreTrainingFile = new ExportScoreTrainingFileStepConfiguration();
        private LdcOnlyAttributesConfiguration ldcOnlyAttributes = new LdcOnlyAttributesConfiguration();

        private SetConfigurationForScoringConfiguration setConfigForScoring = new SetConfigurationForScoringConfiguration();
        private GenerateAIRatingWorkflowConfiguration.Builder generateAIRating = new GenerateAIRatingWorkflowConfiguration.Builder();

        private MergeUserRefinedAttributesConfiguration mergeUserRefinedAttributes = new MergeUserRefinedAttributesConfiguration();

        private UseConfiguredModelingAttributesConfiguration.Builder useConfiguredModelingAttributesBuilder = new UseConfiguredModelingAttributesConfiguration.Builder();
        private AttributeCategoryModifierConfiguration.Builder attributeCategoryModifierConfigurationBuilder = new AttributeCategoryModifierConfiguration.Builder();

        private ImportExportS3StepConfiguration modelImportExportS3 = new ImportExportS3StepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            customEventMatchWorkflowConfigurationBuilder.customer(customerSpace);
            importData.setCustomerSpace(customerSpace);
            registerReport.setCustomerSpace(customerSpace);
            modelDataValidationWorkflow.customer(customerSpace);
            dedupEventTable.setCustomerSpace(customerSpace);
            addStandardAttributes.setCustomerSpace(customerSpace);
            ldcOnlyAttributes.setCustomerSpace(customerSpace);
            exportBucketTool.setCustomerSpace(customerSpace);
            exportScoreTrainingFile.setCustomerSpace(customerSpace);
            modelWorkflowBuilder.customer(customerSpace);
            setConfigForScoring.setCustomerSpace(customerSpace);
            generateAIRating.customer(customerSpace);
            mergeUserRefinedAttributes.setCustomerSpace(customerSpace);
            modelImportExportS3.setCustomerSpace(customerSpace);
            useConfiguredModelingAttributesBuilder.customerSpace(customerSpace);
            attributeCategoryModifierConfigurationBuilder.customerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            customEventMatchWorkflowConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            importData.setMicroServiceHostPort(microServiceHostPort);
            registerReport.setMicroServiceHostPort(microServiceHostPort);
            modelDataValidationWorkflow.microServiceHostPort(microServiceHostPort);
            dedupEventTable.setMicroServiceHostPort(microServiceHostPort);

            addStandardAttributes.setMicroServiceHostPort(microServiceHostPort);
            ldcOnlyAttributes.setMicroServiceHostPort(microServiceHostPort);
            modelWorkflowBuilder.microServiceHostPort(microServiceHostPort);

            exportBucketTool.setMicroServiceHostPort(microServiceHostPort);
            exportScoreTrainingFile.setMicroServiceHostPort(microServiceHostPort);
            setConfigForScoring.setMicroServiceHostPort(microServiceHostPort);
            generateAIRating.microServiceHostPort(microServiceHostPort);

            mergeUserRefinedAttributes.setMicroServiceHostPort(microServiceHostPort);
            modelImportExportS3.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder sourceFileName(String sourceFileName) {
            importData.setSourceFileName(sourceFileName);
            return this;
        }

        public Builder dedupType(DedupType dedupType) {
            dedupEventTable.setDedupType(dedupType);
            return this;
        }

        public Builder minPositiveEvents(long minPositiveEvents) {
            modelDataValidationWorkflow.minPositiveEvents(minPositiveEvents);
            return this;
        }

        public Builder minNegativeEvents(long minNegativeEvents) {
            modelDataValidationWorkflow.minNegativeEvents(minNegativeEvents);
            return this;
        }

        public Builder minRows(long minRows) {
            modelDataValidationWorkflow.minRows(minRows);
            return this;
        }

        public Builder sourceType(SourceType sourceType) {
            importData.setSourceType(sourceType);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            customEventMatchWorkflowConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            importData.setInternalResourceHostPort(internalResourceHostPort);
            registerReport.setInternalResourceHostPort(internalResourceHostPort);
            modelDataValidationWorkflow.internalResourceHostPort(internalResourceHostPort);
            dedupEventTable.setInternalResourceHostPort(internalResourceHostPort);
            modelWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            addStandardAttributes.setInternalResourceHostPort(internalResourceHostPort);
            ldcOnlyAttributes.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            setConfigForScoring.setInternalResourceHostPort(internalResourceHostPort);
            generateAIRating.internalResourceHostPort(internalResourceHostPort);
            mergeUserRefinedAttributes.setInternalResourceHostPort(internalResourceHostPort);
            modelImportExportS3.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder importReportNamePrefix(String reportName) {
            registerReport.setReportNamePrefix(reportName);
            return this;
        }

        public Builder eventTableReportNamePrefix(String eventTableReportName) {
            modelDataValidationWorkflow.eventTableReportNamePrefix(eventTableReportName);
            return this;
        }

        public Builder dedupDataFlowBeanName(String beanName) {
            dedupEventTable.setBeanName(beanName);
            return this;
        }

        public Builder userId(String userId) {
            modelWorkflowBuilder.userId(userId);
            configuration.setUserId(userId);
            generateAIRating.userId(userId);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            modelWorkflowBuilder.modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            setConfigForScoring.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            setConfigForScoring.setInputProperties(inputProperties);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            customEventMatchWorkflowConfigurationBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder matchAccountIdColumn(String matchAccountIdColumn) {
            customEventMatchWorkflowConfigurationBuilder.matchAccountIdColumn(matchAccountIdColumn);
            return this;
        }

        public Builder modelingType(CustomEventModelingType customEventModelingType) {
            customEventMatchWorkflowConfigurationBuilder.modelingType(customEventModelingType);
            generateAIRating.modelingType(customEventModelingType);
            String scoreField = CustomEventModelingType.LPI == customEventModelingType ? InterfaceName.Event.name()
                    : ScoreResultField.RawScore.name();
            generateAIRating.scoreField(scoreField);
            setConfigForScoring.setCustomEventModelingType(customEventModelingType);
            modelImportExportS3.setCustomEventModelingType(customEventModelingType);
            return this;
        }

        public Builder cdlEntityType(EntityType cdlEntityType) {
            customEventMatchWorkflowConfigurationBuilder.cdlEntityType(cdlEntityType);
            return this;
        }

        public Builder targetTableName(String targetTableName) {
            modelWorkflowBuilder.targetTableName(targetTableName);
            return this;
        }

        public Builder aiModelId(String aiModelId) {
            modelWorkflowBuilder.aiModelId(aiModelId);
            return this;
        }

        public Builder ratingEngineId(String ratingEngineId) {
            generateAIRating.ratingEngineId(ratingEngineId);
            modelWorkflowBuilder.ratingEngineId(ratingEngineId);
            return this;
        }

        public Builder setUseScorederivation(boolean useScorederivation) {
            generateAIRating.setUseScorederivation(useScorederivation);
            return this;
        }

        public Builder setModelIdFromRecord(boolean setModelIdFromRecord) {
            generateAIRating.setModelIdFromRecord(setModelIdFromRecord);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            customEventMatchWorkflowConfigurationBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder matchQueue(String queue) {
            customEventMatchWorkflowConfigurationBuilder.matchQueue(queue);
            generateAIRating.matchYarnQueue(queue);
            return this;
        }

        public Builder fetchOnly(boolean fetchOnly) {
            customEventMatchWorkflowConfigurationBuilder.fetchOnly(fetchOnly);
            generateAIRating.fetchOnly(fetchOnly);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            customEventMatchWorkflowConfigurationBuilder.dataCloudVersion(dataCloudVersion);
            modelWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            generateAIRating.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            customEventMatchWorkflowConfigurationBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            customEventMatchWorkflowConfigurationBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            customEventMatchWorkflowConfigurationBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder excludePublicDomains(boolean excludePublicDomains) {
            customEventMatchWorkflowConfigurationBuilder.excludePublicDomains(excludePublicDomains);
            modelWorkflowBuilder.excludePublicDomain(excludePublicDomains);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            customEventMatchWorkflowConfigurationBuilder.excludeDataCloudAttrs(exclude);
            modelWorkflowBuilder.excludeDataCloudAttrs(exclude);
            useConfiguredModelingAttributesBuilder.excludeDataCloudAttributes(exclude);
            attributeCategoryModifierConfigurationBuilder.excludeDataCloudAttributes(exclude);
            return this;
        }

        public Builder keepMatchLid(boolean keepLid) {
            customEventMatchWorkflowConfigurationBuilder.keepMatchLid(keepLid);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            customEventMatchWorkflowConfigurationBuilder.skipDedupStep(skipDedupStep);
            dedupEventTable.setSkipStep(skipDedupStep);
            modelWorkflowBuilder.skipDedupStep(skipDedupStep);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            customEventMatchWorkflowConfigurationBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            addStandardAttributes.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            modelWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            generateAIRating.sourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder matchDebugEnabled(boolean matchDebugEnabled) {
            return this;
        }

        public Builder skipStandardTransform(boolean skipTransform) {
            addStandardAttributes.setSkipStep(skipTransform);
            generateAIRating.skipStandardTransform(skipTransform);
            modelWorkflowBuilder.skipStandardTransform(skipTransform);
            return this;
        }

        public Builder modelName(String modelName) {
            modelWorkflowBuilder.modelName(modelName);
            return this;
        }

        public Builder displayName(String displayName) {
            modelWorkflowBuilder.displayName(displayName);
            return this;
        }

        public Builder trainingTableName(String trainingTableName) {
            modelDataValidationWorkflow.sourceTableName(trainingTableName);
            customEventMatchWorkflowConfigurationBuilder.matchInputTableName(trainingTableName);
            modelWorkflowBuilder.trainingTableName(trainingTableName);
            generateAIRating.inputTableName(trainingTableName);
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup,
                List<TransformDefinition> stdTransformDefns) {
            addStandardAttributes.setTransformationGroup(transformationGroup);
            addStandardAttributes.setTransforms(stdTransformDefns);
            modelWorkflowBuilder.transformationGroup(transformationGroup, stdTransformDefns);
            generateAIRating.transformationGroup(transformationGroup, stdTransformDefns);
            return this;
        }

        public Builder enableV2Profiling(boolean v2ProfilingEnabled) {
            modelWorkflowBuilder.enableV2Profiling(v2ProfilingEnabled);
            return this;
        }

        public Builder addProvenanceProperty(ProvenancePropertyName propertyName, Object value) {
            modelWorkflowBuilder.addProvenanceProperty(propertyName, value);
            return this;
        }

        public Builder pivotArtifactPath(String pivotArtifactPath) {
            modelWorkflowBuilder.pivotArtifactPath(pivotArtifactPath);
            return this;
        }

        public Builder moduleName(String moduleName) {
            modelWorkflowBuilder.moduleName(moduleName);
            return this;
        }

        public Builder runTimeParams(Map<String, String> runTimeParams) {
            modelWorkflowBuilder.runTimeParams(runTimeParams);
            addStandardAttributes.setRuntimeParams(runTimeParams);
            return this;
        }

        public Builder dataRules(List<DataRule> dataRules) {
            modelWorkflowBuilder.dataRules(dataRules);
            return this;
        }

        public Builder eventColumn(String eventColumn) {
            modelWorkflowBuilder.setEventColumn(eventColumn);
            generateAIRating.eventColumn(eventColumn);
            return this;
        }

        public Builder isDefaultDataRules(boolean isDefaultDataRules) {
            modelWorkflowBuilder.isDefaultDataRules(isDefaultDataRules);
            return this;
        }

        public Builder saveBucketMetadata() {
            generateAIRating.saveBucketMetadata();
            return this;
        }

        public Builder setActivateModelSummaryByDefault(boolean value) {
            modelWorkflowBuilder.setActivateModelSummaryByDefault(value);
            return this;
        }

        public Builder skipLdcAttributesOnly(boolean skipLdcAttributesOnly) {
            ldcOnlyAttributes.setSkipStep(skipLdcAttributesOnly);
            useConfiguredModelingAttributesBuilder.excludeCDLAttributes(!skipLdcAttributesOnly);
            attributeCategoryModifierConfigurationBuilder.excludeCDLAttributes(!skipLdcAttributesOnly);
            return this;
        }

        public Builder notesContent(String notesContent) {
            modelWorkflowBuilder.notesContent(notesContent);
            return this;
        }

        public Builder idColumnName(String idColumnName, boolean isLPI) {
            modelWorkflowBuilder.idColumnName(idColumnName);
            customEventMatchWorkflowConfigurationBuilder.matchGroupId(idColumnName);
            generateAIRating.matchGroupId(idColumnName);
            generateAIRating.matchJoinInternalId(isLPI ? true : false);
            return this;
        }

        public Builder cdlMultiModel(boolean cdlMultiMode) {
            generateAIRating.cdlMultiModel(cdlMultiMode);
            return this;
        }

        public Builder dataCollectionVersion(DataCollection.Version version) {
            generateAIRating.dataCollectionVersion(version);
            useConfiguredModelingAttributesBuilder.dataCollectionVersion(version);
            modelImportExportS3.setVersion(version);
            return this;
        }

        public Builder setUserRefinedAttributes(Map<String, ColumnMetadata> userRefinedAttributes) {
            mergeUserRefinedAttributes.setUserRefinedAttributes(userRefinedAttributes);
            return this;
        }

        public Builder modelIteration(Integer modelIteration) {
            useConfiguredModelingAttributesBuilder.modelIteration(modelIteration);
            if (ModelWorkflowConfigurationUtils.skipUseConfiguredModelingAttributesStep(modelIteration)) {
                useConfiguredModelingAttributesBuilder.skipStep(false);
            }
            return this;
        }

        public Builder workflowContainerMem(int mb) {
            configuration.setContainerMemoryMB(mb);
            return this;
        }

        public Builder ratingEngineType(RatingEngineType ratingEngineType) {
            useConfiguredModelingAttributesBuilder.ratingEngineType(ratingEngineType);
            return this;
        }

        public CustomEventModelingWorkflowConfiguration build() {
            customEventMatchWorkflowConfigurationBuilder.matchColumnSelection(Predefined.RTS, "1.0");
            exportBucketTool.setUsingDisplayName(false);
            generateAIRating.saveBucketMetadata();

            configuration.setContainerConfiguration("customEventModelingWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(importData);
            configuration.add(registerReport);
            configuration.add(modelDataValidationWorkflow.build());
            configuration.add(customEventMatchWorkflowConfigurationBuilder.build());

            configuration.add(dedupEventTable);
            configuration.add(addStandardAttributes);
            configuration.add(ldcOnlyAttributes);
            configuration.add(modelWorkflowBuilder.build());
            configuration.add(setConfigForScoring);
            configuration.add(generateAIRating.build());
            configuration.add(exportBucketTool);
            configuration.add(exportScoreTrainingFile);
            configuration.add(modelImportExportS3);
            configuration.add(mergeUserRefinedAttributes);
            configuration.add(useConfiguredModelingAttributesBuilder.build());
            configuration.add(attributeCategoryModifierConfigurationBuilder.build());

            return configuration;
        }
    }
}
