package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableFilterConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlTargetTableFilterConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.PivotScoreAndEventConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.SetConfigurationForScoringConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

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
        private CdlMatchAndModelWorkflowConfiguration.Builder cdlMatchAndModelWorkflowBuilder = new CdlMatchAndModelWorkflowConfiguration.Builder();
        private RatingEngineScoreWorkflowConfiguration.Builder ratingEngineScoreWorkflowBuilder = new RatingEngineScoreWorkflowConfiguration.Builder();

        private CreateCdlEventTableConfiguration cdlEventTable = new CreateCdlEventTableConfiguration();
        private CreateCdlEventTableFilterConfiguration cdlEventTableTupleFilter = new CreateCdlEventTableFilterConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();

        private SetConfigurationForScoringConfiguration setConfigForScoring = new SetConfigurationForScoringConfiguration();
        private CreateCdlTargetTableFilterConfiguration cdlTargetTableTupleFilter = new CreateCdlTargetTableFilterConfiguration();
        private PivotScoreAndEventConfiguration pivotScoreAndEvent = new PivotScoreAndEventConfiguration();

        public Builder microServiceHostPort(String microServiceHostPort) {
            cdlMatchAndModelWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            matchDataCloudWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            cdlEventTable.setMicroServiceHostPort(microServiceHostPort);
            cdlEventTableTupleFilter.setMicroServiceHostPort(microServiceHostPort);
            export.setMicroServiceHostPort(microServiceHostPort);
            setConfigForScoring.setMicroServiceHostPort(microServiceHostPort);
            cdlTargetTableTupleFilter.setMicroServiceHostPort(microServiceHostPort);
            ratingEngineScoreWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            pivotScoreAndEvent.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            cdlMatchAndModelWorkflowBuilder.customer(customerSpace);
            matchDataCloudWorkflowBuilder.customer(customerSpace);
            cdlEventTable.setCustomerSpace(customerSpace);
            cdlEventTableTupleFilter.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            setConfigForScoring.setCustomerSpace(customerSpace);
            cdlTargetTableTupleFilter.setCustomerSpace(customerSpace);
            ratingEngineScoreWorkflowBuilder.customer(customerSpace);
            pivotScoreAndEvent.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder filterTableNames(String trainFilterTableName, String eventFilterTableName,
                String targetFilterTableName) {
            cdlEventTableTupleFilter.setTrainFilterTableName(trainFilterTableName);
            cdlEventTableTupleFilter.setEventFilterTableName(eventFilterTableName);
            cdlTargetTableTupleFilter.setTargetFilterTableName(targetFilterTableName);
            return this;
        }

        public Builder filterQueries(EventFrontEndQuery trainQuery, EventFrontEndQuery eventQuery,
                EventFrontEndQuery targetQuery) {
            cdlEventTableTupleFilter.setTrainQuery(trainQuery);
            cdlEventTableTupleFilter.setEventQuery(eventQuery);
            cdlTargetTableTupleFilter.setTargetQuery(targetQuery);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            cdlMatchAndModelWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            matchDataCloudWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            cdlEventTable.setInternalResourceHostPort(internalResourceHostPort);
            cdlEventTableTupleFilter.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            setConfigForScoring.setInternalResourceHostPort(internalResourceHostPort);
            cdlTargetTableTupleFilter.setInternalResourceHostPort(internalResourceHostPort);
            ratingEngineScoreWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            pivotScoreAndEvent.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder userId(String userId) {
            cdlMatchAndModelWorkflowBuilder.userId(userId);
            pivotScoreAndEvent.setUserId(userId);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            cdlMatchAndModelWorkflowBuilder.modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            setConfigForScoring.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            return this;
        }

        public Builder excludePublicDomains(boolean excludePublicDomains) {
            matchDataCloudWorkflowBuilder.excludePublicDomains(excludePublicDomains);
            cdlMatchAndModelWorkflowBuilder.excludePublicDomain(excludePublicDomains);
            return this;
        }

        public Builder setRetainLatticeAccountId(boolean retainLatticeAccountId) {
            matchDataCloudWorkflowBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            cdlMatchAndModelWorkflowBuilder.setRetainLatticeAccountId(retainLatticeAccountId);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            matchDataCloudWorkflowBuilder.excludeDataCloudAttrs(exclude);
            cdlMatchAndModelWorkflowBuilder.excludeDataCloudAttrs(exclude);
            ratingEngineScoreWorkflowBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder skipDedupStep(boolean skipDedupStep) {
            matchDataCloudWorkflowBuilder.skipDedupStep(skipDedupStep);
            cdlMatchAndModelWorkflowBuilder.skipDedupStep(skipDedupStep);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            matchDataCloudWorkflowBuilder.matchRequestSource(matchRequestSource);
            cdlMatchAndModelWorkflowBuilder.matchRequestSource(matchRequestSource);
            ratingEngineScoreWorkflowBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder matchColumnSelection(Predefined predefinedColumnSelection, String selectionVersion) {
            matchDataCloudWorkflowBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            cdlMatchAndModelWorkflowBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            ratingEngineScoreWorkflowBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            matchDataCloudWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            cdlMatchAndModelWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            ratingEngineScoreWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            cdlMatchAndModelWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            cdlEventTable.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            matchDataCloudWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            ratingEngineScoreWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder trainingTableName(String trainingTableName) {
            cdlEventTable.setTargetTableName(trainingTableName);
            matchDataCloudWorkflowBuilder.matchInputTableName(trainingTableName);
            cdlMatchAndModelWorkflowBuilder.trainingTableName(trainingTableName);
            ratingEngineScoreWorkflowBuilder.inputTableName(trainingTableName);
            return this;
        }

        public Builder modelName(String modelName) {
            cdlMatchAndModelWorkflowBuilder.modelName(modelName);
            return this;
        }

        public Builder displayName(String displayName) {
            cdlMatchAndModelWorkflowBuilder.displayName(displayName);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            setConfigForScoring.setInputProperties(inputProperties);
            return this;
        }

        public Builder dataRules(List<DataRule> dataRules) {
            cdlMatchAndModelWorkflowBuilder.dataRules(dataRules);
            return this;
        }

        public Builder isDefaultDataRules(boolean isDefaultDataRules) {
            cdlMatchAndModelWorkflowBuilder.isDefaultDataRules(isDefaultDataRules);
            return this;
        }

        public Builder addProvenanceProperty(ProvenancePropertyName propertyName, Object value) {
            cdlMatchAndModelWorkflowBuilder.addProvenanceProperty(propertyName, value);
            return this;
        }

        public Builder pivotArtifactPath(String pivotArtifactPath) {
            cdlMatchAndModelWorkflowBuilder.pivotArtifactPath(pivotArtifactPath);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            matchDataCloudWorkflowBuilder.matchClientDocument(matchClientDocument);
            cdlMatchAndModelWorkflowBuilder.matchClientDocument(matchClientDocument);
            ratingEngineScoreWorkflowBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            matchDataCloudWorkflowBuilder.matchType(matchCommandType);
            cdlMatchAndModelWorkflowBuilder.matchType(matchCommandType);
            ratingEngineScoreWorkflowBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            matchDataCloudWorkflowBuilder.matchDestTables(destTables);
            cdlMatchAndModelWorkflowBuilder.matchDestTables(destTables);
            ratingEngineScoreWorkflowBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder moduleName(String moduleName) {
            cdlMatchAndModelWorkflowBuilder.moduleName(moduleName);
            return this;
        }

        public Builder enableV2Profiling(boolean v2ProfilingEnabled) {
            cdlMatchAndModelWorkflowBuilder.enableV2Profiling(v2ProfilingEnabled);
            return this;
        }

        public Builder cdlModel(boolean isCdlModel) {
            cdlMatchAndModelWorkflowBuilder.cdlModel(isCdlModel);
            ratingEngineScoreWorkflowBuilder.cdlModel(isCdlModel);
            return this;
        }

        public Builder notesContent(String notesContent) {
            cdlMatchAndModelWorkflowBuilder.notesContent(notesContent);
            return this;
        }

        public Builder matchQueue(String queue) {
            matchDataCloudWorkflowBuilder.matchQueue(queue);
            cdlMatchAndModelWorkflowBuilder.matchQueue(queue);
            ratingEngineScoreWorkflowBuilder.matchQueue(queue);
            return this;
        }

        public Builder setActivateModelSummaryByDefault(boolean value) {
            cdlMatchAndModelWorkflowBuilder.setActivateModelSummaryByDefault(value);
            return this;
        }

        public Builder bucketMetadata(List<BucketMetadata> bucketMetadata) {
            ratingEngineScoreWorkflowBuilder.bucketMetadata(bucketMetadata);
            return this;
        }

        public Builder liftChart(boolean liftChart) {
            ratingEngineScoreWorkflowBuilder.liftChart(liftChart);
            pivotScoreAndEvent.setLiftChart(liftChart);
            return this;
        }

        public Builder aiModelId(String aiModelId) {
            cdlMatchAndModelWorkflowBuilder.aiModelId(aiModelId);
            return this;
        }

        public Builder ratingEngineId(String ratingEngineId) {
            cdlMatchAndModelWorkflowBuilder.ratingEngineId(ratingEngineId);
            return this;
        }

        public Builder setUniqueKeyColumn(String uniqueKeyColumn) {
            ratingEngineScoreWorkflowBuilder.setUniqueKeyColumn(uniqueKeyColumn);
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
            return this;
        }

        public Builder setExpectedValue(boolean expectedValue) {
            cdlMatchAndModelWorkflowBuilder.setExpectedValue(expectedValue);
            cdlEventTableTupleFilter.setExpectedValue(expectedValue);
            ratingEngineScoreWorkflowBuilder.setExpectedValue(expectedValue);
            pivotScoreAndEvent.setExpectedValue(expectedValue);
            return this;
        }

        public RatingEngineImportMatchAndModelWorkflowConfiguration build() {
            export.setUsingDisplayName(Boolean.FALSE);

            configuration.setContainerConfiguration("ratingEngineImportMatchAndModelWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(cdlEventTableTupleFilter);
            configuration.add(cdlEventTable);
            configuration.add(matchDataCloudWorkflowBuilder.build());
            configuration.add(cdlMatchAndModelWorkflowBuilder.build());
            configuration.add(export);
            configuration.add(setConfigForScoring);
            configuration.add(cdlTargetTableTupleFilter);
            configuration.add(ratingEngineScoreWorkflowBuilder.build());
            configuration.add(pivotScoreAndEvent);

            return configuration;
        }

    }
}
