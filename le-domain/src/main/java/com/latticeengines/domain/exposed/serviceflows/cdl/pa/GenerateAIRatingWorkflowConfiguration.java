package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ScoreAggregateFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.AddStandardAttributesConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ComputeLiftDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.PivotScoreAndEventConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RecalculatePercentileScoreDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ScoreStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class GenerateAIRatingWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Scoring.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    @JsonProperty("customEventModelingType")
    private CustomEventModelingType customEventModelingType;

    public CustomEventModelingType getCustomEventModelingType() {
        return customEventModelingType;
    }

    public void setCustomEventModelingType(CustomEventModelingType customEventModelingType) {
        this.customEventModelingType = customEventModelingType;
    }

    public static class Builder {

        private GenerateAIRatingWorkflowConfiguration configuration = new GenerateAIRatingWorkflowConfiguration();

        private GenerateRatingStepConfiguration generateRatingStepConfiguration = new GenerateRatingStepConfiguration();
        private CreateCdlEventTableConfiguration cdlEventTable = new CreateCdlEventTableConfiguration();
        private AddStandardAttributesConfiguration addStandardAttributes = new AddStandardAttributesConfiguration();
        private MatchDataCloudWorkflowConfiguration.Builder match = new MatchDataCloudWorkflowConfiguration.Builder();

        private ScoreStepConfiguration score = new ScoreStepConfiguration();
        private RecalculatePercentileScoreDataFlowConfiguration recalculatePercentile = new RecalculatePercentileScoreDataFlowConfiguration();
        private ScoreAggregateFlowConfiguration scoreAgg = new ScoreAggregateFlowConfiguration();
        private CombineInputTableWithScoreDataFlowConfiguration combineInputWithScores = new CombineInputTableWithScoreDataFlowConfiguration();
        private ComputeLiftDataFlowConfiguration computeLift = new ComputeLiftDataFlowConfiguration();
        private PivotScoreAndEventConfiguration pivotScoreAndEvent = new PivotScoreAndEventConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            generateRatingStepConfiguration.setCustomerSpace(customerSpace);
            cdlEventTable.setCustomerSpace(customerSpace);
            addStandardAttributes.setCustomerSpace(customerSpace);
            match.customer(customerSpace);
            score.setCustomerSpace(customerSpace);
            recalculatePercentile.setCustomerSpace(customerSpace);
            scoreAgg.setCustomerSpace(customerSpace);
            combineInputWithScores.setCustomerSpace(customerSpace);
            computeLift.setCustomerSpace(customerSpace);
            pivotScoreAndEvent.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            generateRatingStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            cdlEventTable.setMicroServiceHostPort(microServiceHostPort);
            addStandardAttributes.setMicroServiceHostPort(microServiceHostPort);
            match.microServiceHostPort(microServiceHostPort);
            score.setMicroServiceHostPort(microServiceHostPort);
            recalculatePercentile.setMicroServiceHostPort(microServiceHostPort);
            scoreAgg.setMicroServiceHostPort(microServiceHostPort);
            combineInputWithScores.setMicroServiceHostPort(microServiceHostPort);
            computeLift.setMicroServiceHostPort(microServiceHostPort);
            pivotScoreAndEvent.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            addStandardAttributes.setInternalResourceHostPort(internalResourceHostPort);
            pivotScoreAndEvent.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder dataCloudVersion(String dataCloudVersion) {
            match.dataCloudVersion(dataCloudVersion);
            return this;
        }

        public Builder matchYarnQueue(String matchYarnQueue) {
            match.matchQueue(matchYarnQueue);
            return this;
        }

        public Builder fetchOnly(boolean fetchOnly) {
            match.fetchOnly(fetchOnly);
            return this;
        }

        public Builder uniqueKeyColumn(String uniqueKeyColumn) {
            score.setUniqueKeyColumn(uniqueKeyColumn);
            return this;
        }

        public Builder matchGroupId(String matchGroupId) {
            match.matchGroupId(matchGroupId);
            return this;
        }

        public Builder cdlMultiModel(boolean cdlMultiMode) {
            combineInputWithScores.setCdlMultiModel(cdlMultiMode);
            return this;
        }

        public Builder idColumnName(String idColumnName) {
            match.idColumnName(idColumnName);
            return this;
        }

        public Builder userId(String userId) {
            computeLift.setUserId(userId);
            return this;
        }

        public Builder dataCollectionVersion(DataCollection.Version version) {
            generateRatingStepConfiguration.setDataCollectionVersion(version);
            cdlEventTable.setDataCollectionVersion(version);
            return this;
        }

        public Builder saveBucketMetadata() {
            computeLift.setSaveBucketMetadata(Boolean.TRUE);
            return this;
        }

        public Builder ratingEngineId(String ratingEngineId) {
            computeLift.setRatingEngineId(ratingEngineId);
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

        public Builder setExpectedValue(boolean expectedValue) {
            if (expectedValue) {
                computeLift.setScoreField(InterfaceName.ExpectedRevenue.name());
            } else {
                computeLift.setScoreField(InterfaceName.RawScore.name());
            }
            pivotScoreAndEvent.setExpectedValue(expectedValue);
            return this;
        }

        public Builder deferSavingBucketedScoreSummaries() {
            pivotScoreAndEvent.setDeferSavingBucketedScoreSummaries(false);
            return this;
        }

        public Builder inputTableName(String tableName) {
            match.matchInputTableName(tableName);
            combineInputWithScores.setDataFlowParams(new CombineInputTableWithScoreParameters(null, tableName));
            return this;
        }

        public Builder transformationGroup(TransformationGroup transformationGroup,
                List<TransformDefinition> stdTransformDefns) {
            addStandardAttributes.setTransformationGroup(transformationGroup);
            addStandardAttributes.setTransforms(stdTransformDefns);
            return this;
        }

        public Builder modelingType(CustomEventModelingType customEventModelingType) {
            configuration.setCustomEventModelingType(customEventModelingType);
            return this;
        }

        public Builder scoreField(String scoreField) {
            computeLift.setScoreField(scoreField);
            return this;
        }

        public Builder sourceSchemaInterpretation(String sourceSchemaInterpretation) {
            addStandardAttributes.setSourceSchemaInterpretation(sourceSchemaInterpretation);
            return this;
        }

        public Builder skipStandardTransform(boolean skipTransform) {
            addStandardAttributes.setSkipStep(skipTransform);
            return this;
        }

        public GenerateAIRatingWorkflowConfiguration build() {
            setCdlEventTableConfig();
            setMatchConfig();
            setAddStandardAttributesConfig();
            configuration.setContainerConfiguration("generateAIRatingWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(generateRatingStepConfiguration);
            configuration.add(cdlEventTable);
            configuration.add(addStandardAttributes);
            configuration.add(match.build());
            configuration.add(score);
            configuration.add(recalculatePercentile);
            configuration.add(scoreAgg);
            configuration.add(combineInputWithScores);
            configuration.add(computeLift);
            configuration.add(pivotScoreAndEvent);
            return configuration;
        }

        private void setCdlEventTableConfig() {
            cdlEventTable.setEventColumn(InterfaceName.Target.name());
        }

        private void setAddStandardAttributesConfig() {
            if (!CustomEventModelingType.LPI.equals(configuration.getCustomEventModelingType())) {
                addStandardAttributes.setSourceSchemaInterpretation(SchemaInterpretation.SalesforceAccount.toString());
            }
        }

        private void setMatchConfig() {
            if (!CustomEventModelingType.LPI.equals(configuration.getCustomEventModelingType())) {
                match.idColumnName(InterfaceName.__Composite_Key__.name());
            }
            match.matchType(MatchStepConfiguration.LDC);
            match.excludePublicDomains(false);
            match.matchRequestSource(MatchRequestSource.SCORING);
            match.matchColumnSelection(ColumnSelection.Predefined.RTS, "");
            match.sourceSchemaInterpretation(null);
            match.setRetainLatticeAccountId(false);
            match.skipDedupStep(true);
            match.matchHdfsPod(null);
        }
    }
}