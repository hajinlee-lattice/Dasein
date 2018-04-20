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
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.GenerateAIRatingWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableFilterConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlTargetTableFilterConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.AddStandardAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.DedupEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.SetConfigurationForScoringConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class RatingEngineImportMatchAndModelWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Modeling.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {
        private RatingEngineImportMatchAndModelWorkflowConfiguration configuration = new RatingEngineImportMatchAndModelWorkflowConfiguration();

        private MatchDataCloudWorkflowConfiguration.Builder matchDataCloudWorkflowBuilder = new MatchDataCloudWorkflowConfiguration.Builder();
        private CdlModelWorkflowConfiguration.Builder cdlModelWorkflowBuilder = new CdlModelWorkflowConfiguration.Builder();
        private RatingEngineScoreWorkflowConfiguration.Builder ratingEngineScoreWorkflowBuilder = new RatingEngineScoreWorkflowConfiguration.Builder();
        private GenerateAIRatingWorkflowConfiguration.Builder generateAIRating = new GenerateAIRatingWorkflowConfiguration.Builder();

        private CreateCdlEventTableConfiguration cdlEventTable = new CreateCdlEventTableConfiguration();
        private CreateCdlEventTableFilterConfiguration cdlEventTableTupleFilter = new CreateCdlEventTableFilterConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();

        private SetConfigurationForScoringConfiguration setConfigForScoring = new SetConfigurationForScoringConfiguration();
        private CreateCdlTargetTableFilterConfiguration cdlTargetTableTupleFilter = new CreateCdlTargetTableFilterConfiguration();

        private DedupEventTableConfiguration dedupEventTable = new DedupEventTableConfiguration();
        private AddStandardAttributesConfiguration addStandardAttributes = new AddStandardAttributesConfiguration();

        public Builder microServiceHostPort(String microServiceHostPort) {
            cdlModelWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            matchDataCloudWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            cdlEventTable.setMicroServiceHostPort(microServiceHostPort);
            cdlEventTableTupleFilter.setMicroServiceHostPort(microServiceHostPort);
            export.setMicroServiceHostPort(microServiceHostPort);
            setConfigForScoring.setMicroServiceHostPort(microServiceHostPort);
            cdlTargetTableTupleFilter.setMicroServiceHostPort(microServiceHostPort);
            ratingEngineScoreWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            generateAIRating.microServiceHostPort(microServiceHostPort);
            dedupEventTable.setMicroServiceHostPort(microServiceHostPort);
            addStandardAttributes.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            cdlModelWorkflowBuilder.customer(customerSpace);
            matchDataCloudWorkflowBuilder.customer(customerSpace);
            cdlEventTable.setCustomerSpace(customerSpace);
            cdlEventTableTupleFilter.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            setConfigForScoring.setCustomerSpace(customerSpace);
            cdlTargetTableTupleFilter.setCustomerSpace(customerSpace);
            ratingEngineScoreWorkflowBuilder.customer(customerSpace);
            generateAIRating.customer(customerSpace);
            dedupEventTable.setCustomerSpace(customerSpace);
            addStandardAttributes.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            cdlModelWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            matchDataCloudWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            cdlEventTable.setInternalResourceHostPort(internalResourceHostPort);
            cdlEventTableTupleFilter.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            setConfigForScoring.setInternalResourceHostPort(internalResourceHostPort);
            cdlTargetTableTupleFilter.setInternalResourceHostPort(internalResourceHostPort);
            ratingEngineScoreWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            dedupEventTable.setInternalResourceHostPort(internalResourceHostPort);
            addStandardAttributes.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder filterTableNames(String trainFilterTableName, String eventFilterTableName,
                String targetFilterTableName) {
            cdlEventTableTupleFilter.setTrainFilterTableName(trainFilterTableName);
            cdlEventTableTupleFilter.setEventFilterTableName(eventFilterTableName);
            cdlTargetTableTupleFilter.setTargetFilterTableName(targetFilterTableName);
            ratingEngineScoreWorkflowBuilder.filterTableName(targetFilterTableName);
            return this;
        }

        public Builder filterQueries(EventFrontEndQuery trainQuery, EventFrontEndQuery eventQuery,
                EventFrontEndQuery targetQuery) {
            cdlEventTableTupleFilter.setTrainQuery(trainQuery);
            cdlEventTableTupleFilter.setEventQuery(eventQuery);
            cdlTargetTableTupleFilter.setTargetQuery(targetQuery);
            ratingEngineScoreWorkflowBuilder.filterQuery(targetQuery);
            return this;
        }

        public Builder dedupDataFlowBeanName(String beanName) {
            dedupEventTable.setBeanName(beanName);
            return this;
        }

        public Builder dedupType(DedupType dedupType) {
            dedupEventTable.setDedupType(dedupType);
            return this;
        }

        public Builder userId(String userId) {
            cdlModelWorkflowBuilder.userId(userId);
            configuration.setUserId(userId);
            generateAIRating.userId(userId);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            cdlModelWorkflowBuilder.modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            setConfigForScoring.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            return this;
        }

        public Builder excludePublicDomains(boolean excludePublicDomains) {
            matchDataCloudWorkflowBuilder.excludePublicDomains(excludePublicDomains);
            cdlModelWorkflowBuilder.excludePublicDomain(excludePublicDomains);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            matchDataCloudWorkflowBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            matchDataCloudWorkflowBuilder.excludeDataCloudAttrs(exclude);
            cdlModelWorkflowBuilder.excludeDataCloudAttrs(exclude);
            ratingEngineScoreWorkflowBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            dedupEventTable.setSkipStep(skipDedupStep);
            matchDataCloudWorkflowBuilder.skipDedupStep(skipDedupStep);
            cdlModelWorkflowBuilder.skipDedupStep(skipDedupStep);
            return this;
        }

        public Builder fetchOnly(boolean fetchOnly) {
            matchDataCloudWorkflowBuilder.fetchOnly(fetchOnly);
            ratingEngineScoreWorkflowBuilder.fetchOnly(fetchOnly);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            matchDataCloudWorkflowBuilder.matchRequestSource(matchRequestSource);
            ratingEngineScoreWorkflowBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder skipImport(boolean skipImport) {
            ratingEngineScoreWorkflowBuilder.skipImport(skipImport);
            return this;
        }

        public Builder matchColumnSelection(Predefined predefinedColumnSelection, String selectionVersion) {
            matchDataCloudWorkflowBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            ratingEngineScoreWorkflowBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            matchDataCloudWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            cdlModelWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            ratingEngineScoreWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            generateAIRating.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            cdlModelWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            cdlEventTable.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            matchDataCloudWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            ratingEngineScoreWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            addStandardAttributes.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder trainingTableName(String trainingTableName) {
            cdlEventTable.setTargetTableName(trainingTableName);
            matchDataCloudWorkflowBuilder.matchInputTableName(trainingTableName);
            cdlModelWorkflowBuilder.trainingTableName(trainingTableName);
            return this;
        }

        public Builder targetTableName(String targetTableName) {
            ratingEngineScoreWorkflowBuilder.inputTableName(targetTableName);
            cdlModelWorkflowBuilder.targetTableName(targetTableName);
            return this;
        }

        public Builder modelName(String modelName) {
            cdlModelWorkflowBuilder.modelName(modelName);
            return this;
        }

        public Builder displayName(String displayName) {
            cdlModelWorkflowBuilder.displayName(displayName);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            setConfigForScoring.setInputProperties(inputProperties);
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup,
                List<TransformDefinition> stdTransformDefns) {
            addStandardAttributes.setTransformationGroup(transformationGroup);
            addStandardAttributes.setTransforms(stdTransformDefns);
            ratingEngineScoreWorkflowBuilder.transformationGroup(transformationGroup);
            ratingEngineScoreWorkflowBuilder.transformDefinitions(stdTransformDefns);
            generateAIRating.transformationGroup(transformationGroup, stdTransformDefns);
            cdlModelWorkflowBuilder.transformationGroup(transformationGroup, stdTransformDefns);
            return this;
        }

        public Builder dataRules(List<DataRule> dataRules) {
            cdlModelWorkflowBuilder.dataRules(dataRules);
            return this;
        }

        public Builder isDefaultDataRules(boolean isDefaultDataRules) {
            cdlModelWorkflowBuilder.isDefaultDataRules(isDefaultDataRules);
            return this;
        }

        public Builder addProvenanceProperty(ProvenancePropertyName propertyName, Object value) {
            cdlModelWorkflowBuilder.addProvenanceProperty(propertyName, value);
            return this;
        }

        public Builder pivotArtifactPath(String pivotArtifactPath) {
            cdlModelWorkflowBuilder.pivotArtifactPath(pivotArtifactPath);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            matchDataCloudWorkflowBuilder.matchClientDocument(matchClientDocument);
            ratingEngineScoreWorkflowBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            matchDataCloudWorkflowBuilder.matchType(matchCommandType);
            ratingEngineScoreWorkflowBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            matchDataCloudWorkflowBuilder.matchDestTables(destTables);
            ratingEngineScoreWorkflowBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder moduleName(String moduleName) {
            cdlModelWorkflowBuilder.moduleName(moduleName);
            return this;
        }

        public Builder enableV2Profiling(boolean v2ProfilingEnabled) {
            cdlModelWorkflowBuilder.enableV2Profiling(v2ProfilingEnabled);
            return this;
        }

        public Builder cdlModel(boolean isCdlModel) {
            cdlModelWorkflowBuilder.cdlModel(isCdlModel);
            ratingEngineScoreWorkflowBuilder.cdlModel(isCdlModel);
            return this;
        }

        public Builder notesContent(String notesContent) {
            cdlModelWorkflowBuilder.notesContent(notesContent);
            return this;
        }

        public Builder matchQueue(String queue) {
            generateAIRating.matchYarnQueue(queue);
            matchDataCloudWorkflowBuilder.matchQueue(queue);
            ratingEngineScoreWorkflowBuilder.matchQueue(queue);
            return this;
        }

        public Builder skipStandardTransform(boolean skipTransform) {
            addStandardAttributes.setSkipStep(skipTransform);
            ratingEngineScoreWorkflowBuilder.skipStandardTransform(skipTransform);
            return this;
        }

        public Builder setActivateModelSummaryByDefault(boolean value) {
            cdlModelWorkflowBuilder.setActivateModelSummaryByDefault(value);
            return this;
        }

        public Builder bucketMetadata(List<BucketMetadata> bucketMetadata) {
            ratingEngineScoreWorkflowBuilder.bucketMetadata(bucketMetadata);
            return this;
        }

        public Builder liftChart(boolean liftChart) {
            ratingEngineScoreWorkflowBuilder.liftChart(liftChart);
            return this;
        }

        public Builder dataCollectionVersion(DataCollection.Version version) {
            cdlEventTable.setDataCollectionVersion(version);
            generateAIRating.dataCollectionVersion(version);
            return this;
        }

        public Builder aiModelId(String aiModelId) {
            cdlModelWorkflowBuilder.aiModelId(aiModelId);
            return this;
        }

        public Builder ratingEngineId(String ratingEngineId) {
            generateAIRating.ratingEngineId(ratingEngineId);
            cdlModelWorkflowBuilder.ratingEngineId(ratingEngineId);
            return this;
        }

        public Builder setUniqueKeyColumn(String uniqueKeyColumn) {
            ratingEngineScoreWorkflowBuilder.setUniqueKeyColumn(uniqueKeyColumn);
            generateAIRating.uniqueKeyColumn(uniqueKeyColumn);
            return this;
        }

        public Builder cdlMultiModel(boolean cdlMultiMode) {
            generateAIRating.cdlMultiModel(cdlMultiMode);
            return this;
        }

        public Builder setUseScorederivation(boolean useScorederivation) {
            ratingEngineScoreWorkflowBuilder.setUseScorederivation(useScorederivation);
            return this;
        }

        public Builder setModelIdFromRecord(boolean setModelIdFromRecord) {
            ratingEngineScoreWorkflowBuilder.setModelIdFromRecord(setModelIdFromRecord);
            return this;
        }

        public Builder setEventColumn(String eventColumn) {
            cdlEventTable.setEventColumn(eventColumn);
            cdlEventTableTupleFilter.setEventColumn(eventColumn);
            ratingEngineScoreWorkflowBuilder.setEventColumn(eventColumn);
            dedupEventTable.setEventColumn(eventColumn);
            return this;
        }

        public Builder setExpectedValue(boolean expectedValue) {
            cdlModelWorkflowBuilder.setExpectedValue(expectedValue);
            cdlEventTableTupleFilter.setExpectedValue(expectedValue);
            generateAIRating.setExpectedValue(expectedValue);
            ratingEngineScoreWorkflowBuilder.setExpectedValue(expectedValue);
            return this;
        }

        public RatingEngineImportMatchAndModelWorkflowConfiguration build() {
            export.setUsingDisplayName(Boolean.FALSE);
            generateAIRating.saveBucketMetadata();
            generateAIRating.fetchOnly(Boolean.TRUE);

            configuration.setContainerConfiguration("ratingEngineImportMatchAndModelWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(cdlEventTableTupleFilter);
            configuration.add(cdlEventTable);
            configuration.add(dedupEventTable);
            configuration.add(addStandardAttributes);
            configuration.add(matchDataCloudWorkflowBuilder.build());
            configuration.add(cdlModelWorkflowBuilder.build());
            configuration.add(export);
            configuration.add(setConfigForScoring);
            configuration.add(cdlTargetTableTupleFilter);
            configuration.add(ratingEngineScoreWorkflowBuilder.build());
            configuration.add(generateAIRating.build());

            return configuration;
        }

    }
}
