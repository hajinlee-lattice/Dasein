package com.latticeengines.propdata.workflow.match;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.propdata.dataflow.CascadingBulkMatchDataflowParameters;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.propdata.workflow.match.steps.CascadingBulkMatchStepConfiguration;

public class CascadingBulkMatchWorkflowConfiguration extends WorkflowConfiguration {

    public static class Builder {

        private CascadingBulkMatchWorkflowConfiguration configuration = new CascadingBulkMatchWorkflowConfiguration();
        private CascadingBulkMatchStepConfiguration stepConfigiguraton = new CascadingBulkMatchStepConfiguration();
        private CustomerSpace customerSpace;

        public Builder rootOperationUid(String rootUid) {
            stepConfigiguraton.setRootOperationUid(rootUid);
            return this;
        }

        public Builder hdfsPodId(String hdfsPodId) {
            stepConfigiguraton.setHdfsPodId(hdfsPodId);
            return this;
        }

        public Builder matchInput(MatchInput matchInput) {
            customerSpace = CustomerSpace.parse(matchInput.getTenant().getId());
            stepConfigiguraton.setCustomerSpace(customerSpace);
            stepConfigiguraton.setYarnQueue(matchInput.getYarnQueue());
            return this;
        }

        public Builder inputProperties() {
            Map<String, String> inputProperties = new HashMap<>();
            inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "cascadingBulkMatchWorkflow");
            stepConfigiguraton.setInputProperties(inputProperties);
            return this;
        }

        public Builder dataflowParameter(CascadingBulkMatchDataflowParameters dataflowParameters) {
            stepConfigiguraton.setDataFlowParams(dataflowParameters);
            return this;
        }

        public Builder targetPath(String outputDir) {
            stepConfigiguraton.setTargetPath(outputDir);
            return this;
        }

        public Builder dataflowExtraSources(Map<String, String> dataflowExtaSources) {
            stepConfigiguraton.setExtraSources(dataflowExtaSources);
            return this;
        }

        public Builder targetTableName(String targetTableName) {
            stepConfigiguraton.setTargetTableName(targetTableName);
            return this;
        }

        public Builder setBeanName(String string) {
            stepConfigiguraton.setBeanName("cascadingBulkMatchDataflow");
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            stepConfigiguraton.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public CascadingBulkMatchWorkflowConfiguration build() {
            configuration.setContainerConfiguration("cascadingBulkMatchWorkflow", customerSpace,
                    "CascadingBulkMatchWorkflow");
            configuration.add(stepConfigiguraton);
            return configuration;
        }

    }

}