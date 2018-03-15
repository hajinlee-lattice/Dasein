package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.MatchJoinType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlTargetTableFilterConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ScoreAggregateFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.AddStandardAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ScoreStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class RatingEngineScoreWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    private RatingEngineScoreWorkflowConfiguration() {
    }

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Scoring.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    private boolean skipImport;

    public boolean isSkipImport() {
        return skipImport;
    }

    public boolean setSkipImport(boolean skipImport) {
        return this.skipImport = skipImport;
    }

    public static class Builder {

        private RatingEngineScoreWorkflowConfiguration configuration = new RatingEngineScoreWorkflowConfiguration();
        private MicroserviceStepConfiguration microserviceStepConfiguration = new MicroserviceStepConfiguration();
        private MatchDataCloudWorkflowConfiguration.Builder matchDataCloudWorkflowBuilder = new MatchDataCloudWorkflowConfiguration.Builder();

        private ScoreStepConfiguration score = new ScoreStepConfiguration();
        private CombineInputTableWithScoreDataFlowConfiguration combineInputWithScores = new CombineInputTableWithScoreDataFlowConfiguration();
        private ScoreAggregateFlowConfiguration scoreAggregate = new ScoreAggregateFlowConfiguration();
        private ExportStepConfiguration export = new ExportStepConfiguration();

        private CreateCdlTargetTableFilterConfiguration cdlTargetTableTupleFilter = new CreateCdlTargetTableFilterConfiguration();
        private CreateCdlEventTableConfiguration cdlEventTable = new CreateCdlEventTableConfiguration();

        private AddStandardAttributesConfiguration addStandardAttributes = new AddStandardAttributesConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("ratingEngineScoreWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            microserviceStepConfiguration.setCustomerSpace(customerSpace);
            matchDataCloudWorkflowBuilder.customer(customerSpace);
            score.setCustomerSpace(customerSpace);
            combineInputWithScores.setCustomerSpace(customerSpace);
            scoreAggregate.setCustomerSpace(customerSpace);
            export.setCustomerSpace(customerSpace);
            cdlTargetTableTupleFilter.setCustomerSpace(customerSpace);
            cdlEventTable.setCustomerSpace(customerSpace);
            addStandardAttributes.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            microserviceStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            matchDataCloudWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            cdlTargetTableTupleFilter.setMicroServiceHostPort(microServiceHostPort);
            cdlEventTable.setMicroServiceHostPort(microServiceHostPort);
            addStandardAttributes.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            matchDataCloudWorkflowBuilder.internalResourceHostPort(internalResourceHostPort);
            score.setInternalResourceHostPort(internalResourceHostPort);
            combineInputWithScores.setInternalResourceHostPort(internalResourceHostPort);
            scoreAggregate.setInternalResourceHostPort(internalResourceHostPort);
            export.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            cdlTargetTableTupleFilter.setInternalResourceHostPort(internalResourceHostPort);
            cdlEventTable.setInternalResourceHostPort(internalResourceHostPort);
            addStandardAttributes.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder inputTableName(String tableName) {
            matchDataCloudWorkflowBuilder.matchInputTableName(tableName);
            cdlEventTable.setTargetTableName(tableName);
            combineInputWithScores.setDataFlowParams(new CombineInputTableWithScoreParameters(null, tableName));
            return this;
        }

        public Builder filterTableName(String targetFilterTableName) {
            cdlTargetTableTupleFilter.setTargetFilterTableName(targetFilterTableName);
            return this;
        }

        public Builder filterQuery(EventFrontEndQuery targetQuery) {
            cdlTargetTableTupleFilter.setTargetQuery(targetQuery);
            return this;
        }

        public Builder modelId(String modelId) {
            score.setModelId(modelId);
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup) {
            addStandardAttributes.setTransformationGroup(transformationGroup);
            return this;
        }

        public Builder transformDefinitions(List<TransformDefinition> transforms) {
            addStandardAttributes.setTransforms(transforms);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            matchDataCloudWorkflowBuilder.sourceSchemaInterpretation(sourceSchemaInterpretation);
            addStandardAttributes.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder excludeDataCloudAttrs(boolean exclude) {
            matchDataCloudWorkflowBuilder.excludeDataCloudAttrs(exclude);
            return this;
        }

        public Builder matchClientDocument(MatchClientDocument matchClientDocument) {
            matchDataCloudWorkflowBuilder.matchClientDocument(matchClientDocument);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            matchDataCloudWorkflowBuilder.matchType(matchCommandType);
            return this;
        }

        public Builder matchDestTables(String destTables) {
            matchDataCloudWorkflowBuilder.matchDestTables(destTables);
            return this;
        }

        public Builder outputFilename(String outputFilename) {
            export.setExportDestination(ExportDestination.FILE);
            export.putProperty(ExportProperty.TARGET_FILE_NAME, outputFilename);
            return this;
        }

        public Builder outputFileFormat(ExportFormat format) {
            export.setExportFormat(format);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder matchJoinType(MatchJoinType matchJoinType) {
            matchDataCloudWorkflowBuilder.matchJoinType(matchJoinType);
            return this;
        }

        public Builder columnSelection(Predefined predefinedColumnSelection, String selectionVersion) {
            matchDataCloudWorkflowBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            matchDataCloudWorkflowBuilder.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchRequestSource(MatchRequestSource matchRequestSource) {
            matchDataCloudWorkflowBuilder.matchRequestSource(matchRequestSource);
            return this;
        }

        public Builder bucketMetadata(List<BucketMetadata> bucketMetadataList) {
            combineInputWithScores.setBucketMetadata(bucketMetadataList);
            return this;
        }

        public Builder matchQueue(String queue) {
            matchDataCloudWorkflowBuilder.matchQueue(queue);
            return this;
        }

        public Builder setUniqueKeyColumn(String uniqueKeyColumn) {
            score.setUniqueKeyColumn(uniqueKeyColumn);
            return this;
        }

        public Builder setUseScorederivation(boolean useScorederivation) {
            score.setUseScorederivation(useScorederivation);
            return this;
        }

        public Builder setModelIdFromRecord(boolean setModelIdFromRecord) {
            score.setModelIdFromRecord(setModelIdFromRecord);
            return this;
        }

        public Builder setEventColumn(String eventColumn) {
            cdlEventTable.setEventColumn(eventColumn);
            return this;
        }

        public Builder cdlModel(boolean isCdlModel) {
            combineInputWithScores.setCdlModel(isCdlModel);
            return this;
        }

        public Builder setExpectedValue(boolean expectedValue) {
            combineInputWithScores.setExpectedValue(expectedValue);
            scoreAggregate.setExpectedValue(expectedValue);
            return this;
        }

        public Builder liftChart(boolean liftChart) {
            combineInputWithScores.setLiftChart(liftChart);
            return this;
        }

        public Builder matchColumnSelection(Predefined predefinedColumnSelection, String selectionVersion) {
            matchDataCloudWorkflowBuilder.matchColumnSelection(predefinedColumnSelection, selectionVersion);
            return this;
        }

        public RatingEngineScoreWorkflowConfiguration build() {
            score.microserviceStepConfiguration(microserviceStepConfiguration);
            combineInputWithScores.microserviceStepConfiguration(microserviceStepConfiguration);
            scoreAggregate.microserviceStepConfiguration(microserviceStepConfiguration);
            addStandardAttributes.microserviceStepConfiguration(microserviceStepConfiguration);
            
            export.microserviceStepConfiguration(microserviceStepConfiguration);
            cdlTargetTableTupleFilter.microserviceStepConfiguration(microserviceStepConfiguration);
            cdlEventTable.microserviceStepConfiguration(microserviceStepConfiguration);

            configuration.add(cdlTargetTableTupleFilter);
            configuration.add(cdlEventTable);
            configuration.add(matchDataCloudWorkflowBuilder.build());
            configuration.add(score);
            configuration.add(addStandardAttributes);
            configuration.add(combineInputWithScores);
            configuration.add(scoreAggregate);
            configuration.add(export);

            return configuration;
        }

    }
}
