package com.latticeengines.domain.exposed.serviceflows.prospectdiscovery;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MatchStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ChooseModelStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.modeling.steps.ModelStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.CreatePreMatchEventTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.RunAttributeLevelSummaryDataFlowsConfiguration;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.RunImportSummaryDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.RunScoreTableDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.TargetMarketStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.ScoreStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class FitModelWorkflowConfiguration extends BasePDWorkflowConfiguration {

    private FitModelWorkflowConfiguration() {
    }

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Modeling.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {

        private FitModelWorkflowConfiguration fitModel = new FitModelWorkflowConfiguration();
        private MicroserviceStepConfiguration microservice = new MicroserviceStepConfiguration();
        private ImportStepConfiguration importData = new ImportStepConfiguration();
        private CreatePreMatchEventTableConfiguration preMatchDataFlow = new CreatePreMatchEventTableConfiguration();
        private MatchStepConfiguration match = new MatchStepConfiguration();
        private RunImportSummaryDataFlowConfiguration runImportSummaryDataFlow = new RunImportSummaryDataFlowConfiguration();
        private ModelStepConfiguration model = new ModelStepConfiguration();
        private ChooseModelStepConfiguration chooseModel = new ChooseModelStepConfiguration();
        private ScoreStepConfiguration score = new ScoreStepConfiguration();
        private TargetMarketStepConfiguration targetMarketConfiguration = new TargetMarketStepConfiguration();
        private RunScoreTableDataFlowConfiguration runScoreTableDataFlow = new RunScoreTableDataFlowConfiguration();
        private RunAttributeLevelSummaryDataFlowsConfiguration attrLevelSummaryDataFlows = new RunAttributeLevelSummaryDataFlowsConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            fitModel.setContainerConfiguration("fitModelWorkflow", customerSpace, fitModel.getClass().getName());
            microservice.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            microservice.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder sourceType(SourceType sourceType) {
            importData.setSourceType(sourceType);
            return this;
        }

        public Builder targetMarket(TargetMarket targetMarket) {
            chooseModel.setTargetMarket(targetMarket);
            targetMarketConfiguration.setTargetMarket(targetMarket);
            attrLevelSummaryDataFlows.setTargetMarket(targetMarket);
            return this;
        }

        public Builder extraSources(Map<String, String> extraSources) {
            preMatchDataFlow.setExtraSources(extraSources);
            return this;
        }

        public Builder matchDbUrl(String matchDbUrl) {
            match.setDbUrl(matchDbUrl);
            return this;
        }

        public Builder matchDbUser(String matchDbUser) {
            match.setDbUser(matchDbUser);
            return this;
        }

        public Builder matchDbPasswordEncrypted(String matchDbPasswordEncrypted) {
            match.setDbPasswordEncrypted(matchDbPasswordEncrypted);
            return this;
        }

        public Builder matchDestTables(String matchDestTables) {
            match.setDestTables(matchDestTables);
            return this;
        }

        public Builder matchClient(String matchClient) {
            match.setMatchClient(matchClient);
            return this;
        }

        public Builder matchType(MatchCommandType matchCommandType) {
            match.setMatchCommandType(matchCommandType);
            return this;
        }

        public Builder prematchFlowTableName(String prematchFlowTableName) {
            match.setInputTableName(prematchFlowTableName);
            return this;
        }

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            model.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            chooseModel.setInternalResourceHostPort(internalResourceHostPort);
            targetMarketConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            attrLevelSummaryDataFlows.setInternalResourceHostPort(internalResourceHostPort);
            model.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder uniqueKeyColumn(String uniqueKeyColumn) {
            score.setUniqueKeyColumn(uniqueKeyColumn);
            runScoreTableDataFlow.setUniqueKeyColumn(uniqueKeyColumn);
            return this;
        }

        public Builder modelName(String modelName) {
            model.setModelName(modelName);
            return this;
        }

        public Builder directoryToScore(String directoryToScore) {
            score.setSourceDir(directoryToScore);
            runScoreTableDataFlow.setAccountMasterNameAndPath(new String[] { "AccountMaster", directoryToScore });
            return this;
        }

        public Builder modelId(String modelId) {
            score.setModelId(modelId);
            return this;
        }

        public Builder registerScoredTable(Boolean registerScoredTable) {
            score.setRegisterScoredTable(registerScoredTable);
            return this;
        }

        public Builder attributes(List<String> attributes) {
            attrLevelSummaryDataFlows.setAttributes(attributes);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            fitModel.setInputProperties(inputProperties);
            return this;
        }

        public FitModelWorkflowConfiguration build() {
            importData.microserviceStepConfiguration(microservice);
            preMatchDataFlow.microserviceStepConfiguration(microservice);
            match.microserviceStepConfiguration(microservice);
            runImportSummaryDataFlow.microserviceStepConfiguration(microservice);
            model.microserviceStepConfiguration(microservice);
            chooseModel.microserviceStepConfiguration(microservice);
            score.microserviceStepConfiguration(microservice);
            targetMarketConfiguration.microserviceStepConfiguration(microservice);
            runScoreTableDataFlow.microserviceStepConfiguration(microservice);
            attrLevelSummaryDataFlows.microserviceStepConfiguration(microservice);
            fitModel.add(microservice);
            fitModel.add(importData);
            fitModel.add(preMatchDataFlow);
            fitModel.add(match);
            fitModel.add(runImportSummaryDataFlow);
            fitModel.add(model);
            fitModel.add(chooseModel);
            fitModel.add(score);
            fitModel.add(targetMarketConfiguration);
            fitModel.add(runScoreTableDataFlow);
            fitModel.add(attrLevelSummaryDataFlows);

            return fitModel;
        }
    }

}
