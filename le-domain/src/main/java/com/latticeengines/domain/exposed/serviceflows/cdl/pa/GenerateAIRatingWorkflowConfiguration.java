package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Collection;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CreateCdlEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ScoreAggregateFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.MatchDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ComputeLiftDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ScoreStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class GenerateAIRatingWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Scoring.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {

        private GenerateAIRatingWorkflowConfiguration configuration = new GenerateAIRatingWorkflowConfiguration();

        private GenerateRatingStepConfiguration generateRatingStepConfiguration = new GenerateRatingStepConfiguration();
        private CreateCdlEventTableConfiguration cdlEventTable = new CreateCdlEventTableConfiguration();
        private MatchDataCloudWorkflowConfiguration.Builder matchDataCloudWorkflowBuilder = new MatchDataCloudWorkflowConfiguration.Builder();

        private ScoreStepConfiguration score = new ScoreStepConfiguration();
        private ScoreAggregateFlowConfiguration scoreAgg = new ScoreAggregateFlowConfiguration();
        private CombineInputTableWithScoreDataFlowConfiguration combineInputWithScores = new CombineInputTableWithScoreDataFlowConfiguration();
        private ComputeLiftDataFlowConfiguration computeLift = new ComputeLiftDataFlowConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            generateRatingStepConfiguration.setCustomerSpace(customerSpace);
            cdlEventTable.setCustomerSpace(customerSpace);
            matchDataCloudWorkflowBuilder.customer(customerSpace);
            score.setCustomerSpace(customerSpace);
            scoreAgg.setCustomerSpace(customerSpace);
            combineInputWithScores.setCustomerSpace(customerSpace);
            computeLift.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            generateRatingStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            cdlEventTable.setMicroServiceHostPort(microServiceHostPort);
            matchDataCloudWorkflowBuilder.microServiceHostPort(microServiceHostPort);
            score.setMicroServiceHostPort(microServiceHostPort);
            scoreAgg.setMicroServiceHostPort(microServiceHostPort);
            combineInputWithScores.setMicroServiceHostPort(microServiceHostPort);
            computeLift.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder dataCloudVersion(DataCloudVersion dataCloudVersion) {
            matchDataCloudWorkflowBuilder.dataCloudVersion(dataCloudVersion.getVersion());
            return this;
        }

        public Builder matchYarnQueue(String matchYarnQueue) {
            matchDataCloudWorkflowBuilder.matchQueue(matchYarnQueue);
            return this;
        }

        public GenerateAIRatingWorkflowConfiguration build() {
            setCdlEventTableConfig();
            setMatchConfig();
            setScoreConfig();
            configuration.setContainerConfiguration("generateAIRatingWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(generateRatingStepConfiguration);
            configuration.add(cdlEventTable);
            configuration.add(matchDataCloudWorkflowBuilder.build());
            configuration.add(score);
            configuration.add(scoreAgg);
            configuration.add(combineInputWithScores);
            configuration.add(computeLift);
            return configuration;
        }

        private void setCdlEventTableConfig() {
            cdlEventTable.setEventColumn(InterfaceName.Target.name());
        }

        private void setMatchConfig() {
            matchDataCloudWorkflowBuilder.excludePublicDomains(false);
            matchDataCloudWorkflowBuilder.matchRequestSource(MatchRequestSource.SCORING);
            matchDataCloudWorkflowBuilder.matchColumnSelection(ColumnSelection.Predefined.RTS, "");
            matchDataCloudWorkflowBuilder.sourceSchemaInterpretation(null);
            matchDataCloudWorkflowBuilder.setRetainLatticeAccountId(false);
            matchDataCloudWorkflowBuilder.skipDedupStep(true);
            matchDataCloudWorkflowBuilder.matchHdfsPod(null);
        }

        private void setScoreConfig() {
            score.setUniqueKeyColumn(InterfaceName.__Composite_Key__.name());
            score.setModelIdFromRecord(true);
            score.setUseScorederivation(false);
            combineInputWithScores.setCdlMultiModel(true);
        }

    }
}