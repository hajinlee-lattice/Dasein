package com.latticeengines.domain.exposed.serviceflows.prospectdiscovery;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.RunAttributeLevelSummaryDataFlowsConfiguration;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.RunScoreTableDataFlowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.TargetMarketStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class CreateAttributeLevelSummaryWorkflowConfiguration extends BasePDWorkflowConfiguration {

    private CreateAttributeLevelSummaryWorkflowConfiguration() {
    }

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Scoring.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {

        private CreateAttributeLevelSummaryWorkflowConfiguration attributeSummary = new CreateAttributeLevelSummaryWorkflowConfiguration();
        private MicroserviceStepConfiguration microservice = new MicroserviceStepConfiguration();
        private RunScoreTableDataFlowConfiguration runScoreTableDataFlow = new RunScoreTableDataFlowConfiguration();
        private RunAttributeLevelSummaryDataFlowsConfiguration attrLevelSummaryDataFlows = new RunAttributeLevelSummaryDataFlowsConfiguration();
        private TargetMarketStepConfiguration targetMarketConfiguration = new TargetMarketStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            attributeSummary.setCustomerSpace(customerSpace);
            microservice.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            microservice.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder targetMarket(TargetMarket targetMarket) {
            targetMarketConfiguration.setTargetMarket(targetMarket);
            attrLevelSummaryDataFlows.setTargetMarket(targetMarket);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            targetMarketConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            attrLevelSummaryDataFlows.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder accountMasterNameAndPath(String[] accountMasterAndPath) {
            runScoreTableDataFlow.setAccountMasterNameAndPath(accountMasterAndPath);
            return this;
        }

        public Builder scoreResult(String scoreResult) {
            runScoreTableDataFlow.setScoreResult(scoreResult);
            return this;
        }

        public Builder uniqueKeyColumn(String uniqueKeyColumn) {
            runScoreTableDataFlow.setUniqueKeyColumn(uniqueKeyColumn);
            return this;
        }

        public Builder attributes(List<String> attributes) {
            attrLevelSummaryDataFlows.setAttributes(attributes);
            return this;
        }

        public Builder eventTableName(String eventTableName) {
            attrLevelSummaryDataFlows.setEventTableName(eventTableName);
            return this;
        }

        public Builder eventColumnName(String eventColumnName) {
            attrLevelSummaryDataFlows.setEventColumnName(eventColumnName);
            return this;
        }

        public CreateAttributeLevelSummaryWorkflowConfiguration build() {
            runScoreTableDataFlow.microserviceStepConfiguration(microservice);
            targetMarketConfiguration.microserviceStepConfiguration(microservice);
            attrLevelSummaryDataFlows.microserviceStepConfiguration(microservice);

            attributeSummary.add(microservice);
            attributeSummary.add(attrLevelSummaryDataFlows);
            attributeSummary.add(runScoreTableDataFlow);
            attributeSummary.add(targetMarketConfiguration);

            return attributeSummary;
        }
    }

}
