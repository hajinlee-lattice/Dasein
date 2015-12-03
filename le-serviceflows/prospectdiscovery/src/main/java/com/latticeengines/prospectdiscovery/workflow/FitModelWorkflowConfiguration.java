package com.latticeengines.prospectdiscovery.workflow;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;
import com.latticeengines.serviceflows.workflow.importdata.ImportStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchStepConfiguration;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;

public class FitModelWorkflowConfiguration extends WorkflowConfiguration {

    private FitModelWorkflowConfiguration() {
    }

    public static class Builder {

        private FitModelWorkflowConfiguration fitModel = new FitModelWorkflowConfiguration();
        private MicroserviceStepConfiguration microservice = new MicroserviceStepConfiguration();
        private ImportStepConfiguration importData = new ImportStepConfiguration();
        private DataFlowStepConfiguration dataFlow = new DataFlowStepConfiguration();
        private MatchStepConfiguration match = new MatchStepConfiguration();
        private ModelStepConfiguration model = new ModelStepConfiguration();

        public Builder customer(String customerSpace) {
            fitModel.setCustomerSpace(CustomerSpace.parse(customerSpace));
            microservice.setCustomerSpace(customerSpace);
            fitModel.setCustomerSpace(CustomerSpace.parse(customerSpace));
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

        public Builder targetPath(String targetPath) {
            dataFlow.setTargetPath(targetPath);
            return this;
        }

        public Builder targetMarket(TargetMarket targetMarket) {
            // TODO Set target market on post-match flow configuration
            return this;
        }

        public Builder extraSources(List<String> extraSources) {
            dataFlow.setExtraSources(extraSources);
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

        public Builder modelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
            model.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
            return this;
        }

        public Builder eventColumns(List<String> eventColumns) {
            model.setEventColumns(eventColumns);
            return this;
        }

        public FitModelWorkflowConfiguration build() {
            importData.microserviceStepConfiguration(microservice);
            dataFlow.microserviceStepConfiguration(microservice);
            match.microserviceStepConfiguration(microservice);
            model.microserviceStepConfiguration(microservice);

            fitModel.add(microservice);
            fitModel.add(importData);
            fitModel.add(dataFlow);
            fitModel.add(match);
            fitModel.add(model);

            return fitModel;
        }
    }

}
