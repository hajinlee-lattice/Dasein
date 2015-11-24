package com.latticeengines.prospectdiscovery.workflow;

import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;
import com.latticeengines.serviceflows.workflow.importdata.ImportStepConfiguration;
import com.latticeengines.serviceflows.workflow.match.MatchStepConfiguration;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;
import com.latticeengines.workflow.exposed.build.WorkflowConfiguration;

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

        public Builder flowName(String flowName) {
            dataFlow.setFlowName(flowName);
            match.setFlowName(flowName);
            return this;
        }

        public Builder dataflowBeanName(String dataflowBeanName) {
            dataFlow.setDataflowBeanName(dataflowBeanName);
            return this;
        }

        public Builder targetPath(String targetPath) {
            dataFlow.setTargetPath(targetPath);
            return this;
        }

        public Builder stoplistAvroFile(String stoplistAvroFile) {
            dataFlow.setStoplistAvroFile(stoplistAvroFile);
            return this;
        }

        public Builder stoplistPath(String stoplistPath) {
            dataFlow.setStoplistPath(stoplistPath);
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

        public Builder matchDbPassword(String matchDbPassword) {
            match.setDbPassword(matchDbPassword);
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
