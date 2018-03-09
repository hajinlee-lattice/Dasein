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
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ProcessMatchResultConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ComputeLiftDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ScoreStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class GenerateRatingWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    private GenerateRatingWorkflowConfiguration() {

    }

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Scoring.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {

        public GenerateRatingWorkflowConfiguration configuration = new GenerateRatingWorkflowConfiguration();

        private GenerateRatingStepConfiguration generateRatingStepConfiguration = new GenerateRatingStepConfiguration();
        private CreateCdlEventTableConfiguration cdlEventTable = new CreateCdlEventTableConfiguration();
        private MatchStepConfiguration match = new MatchStepConfiguration();
        private ProcessMatchResultConfiguration matchResult = new ProcessMatchResultConfiguration();
        private ScoreStepConfiguration score = new ScoreStepConfiguration();
        private ScoreAggregateFlowConfiguration scoreAgg = new ScoreAggregateFlowConfiguration();
        private CombineInputTableWithScoreDataFlowConfiguration combineInputWithScores = new CombineInputTableWithScoreDataFlowConfiguration();
        private ComputeLiftDataFlowConfiguration computeLift = new ComputeLiftDataFlowConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            generateRatingStepConfiguration.setCustomerSpace(customerSpace);
            cdlEventTable.setCustomerSpace(customerSpace);
            match.setCustomerSpace(customerSpace);
            matchResult.setCustomerSpace(customerSpace);
            score.setCustomerSpace(customerSpace);
            scoreAgg.setCustomerSpace(customerSpace);
            combineInputWithScores.setCustomerSpace(customerSpace);
            computeLift.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            generateRatingStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            cdlEventTable.setMicroServiceHostPort(microServiceHostPort);
            match.setMicroServiceHostPort(microServiceHostPort);
            matchResult.setMicroServiceHostPort(microServiceHostPort);
            score.setMicroServiceHostPort(microServiceHostPort);
            scoreAgg.setMicroServiceHostPort(microServiceHostPort);
            combineInputWithScores.setMicroServiceHostPort(microServiceHostPort);
            computeLift.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder dataCloudVersion(DataCloudVersion dataCloudVersion) {
            match.setDataCloudVersion(dataCloudVersion.getVersion());
            return this;
        }

        public Builder matchYarnQueue(String matchYarnQueue) {
            match.setMatchQueue(matchYarnQueue);
            return this;
        }

        public GenerateRatingWorkflowConfiguration build() {
            setCdlEventTableConfig();
            setMatchConfig();
            setScoreConfig();
            configuration.setContainerConfiguration("processAnalyzeWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(generateRatingStepConfiguration);
            configuration.add(cdlEventTable);
            configuration.add(match);
            configuration.add(matchResult);
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
            match.setExcludePublicDomain(false);
            match.setMatchRequestSource(MatchRequestSource.SCORING);
            match.setPredefinedColumnSelection(ColumnSelection.Predefined.RTS);
            match.setSourceSchemaInterpretation(null);
            match.setMatchHdfsPod(null);
            match.setRetainLatticeAccountId(false);

            match.setSkipDedupe(true);
            matchResult.setSkipDedupe(true);
        }

        private void setScoreConfig() {
            score.setUniqueKeyColumn(InterfaceName.__Composite_Key__.name());
            score.setModelIdFromRecord(true);
            score.setUseScorederivation(false);
            combineInputWithScores.setCdlMultiModel(true);
        }

    }
}
