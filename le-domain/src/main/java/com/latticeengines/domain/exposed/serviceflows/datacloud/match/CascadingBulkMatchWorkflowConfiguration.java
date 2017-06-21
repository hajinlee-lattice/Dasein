package com.latticeengines.domain.exposed.serviceflows.datacloud.match;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.CascadingBulkMatchDataflowParameters;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.serviceflows.datacloud.BaseDataCloudWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.CascadingBulkMatchStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

public class CascadingBulkMatchWorkflowConfiguration extends BaseDataCloudWorkflowConfiguration {

    public static class Builder {

        private CascadingBulkMatchWorkflowConfiguration configuration = new CascadingBulkMatchWorkflowConfiguration();
        private CascadingBulkMatchStepConfiguration stepConfiguration = new CascadingBulkMatchStepConfiguration();
        private CustomerSpace customerSpace;

        public Builder rootOperationUid(String rootUid) {
            stepConfiguration.setRootOperationUid(rootUid);
            return this;
        }

        public Builder hdfsPodId(String hdfsPodId) {
            stepConfiguration.setHdfsPodId(hdfsPodId);
            return this;
        }

        public Builder matchInput(MatchInput matchInput) {
            customerSpace = CustomerSpace.parse(matchInput.getTenant().getId());
            stepConfiguration.setCustomerSpace(customerSpace);
            stepConfiguration.setYarnQueue(matchInput.getYarnQueue());
            return this;
        }

        public Builder inputProperties() {
            Map<String, String> inputProperties = new HashMap<>();
            inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "cascadingBulkMatchWorkflow");
            stepConfiguration.setInputProperties(inputProperties);
            return this;
        }

        public Builder dataflowParameter(CascadingBulkMatchDataflowParameters dataflowParameters) {
            stepConfiguration.setDataFlowParams(dataflowParameters);
            return this;
        }

        public Builder targetPath(String outputDir) {
            stepConfiguration.setTargetPath(outputDir);
            return this;
        }

        public Builder partitions(Integer partitions) {
            stepConfiguration.setPartitions(partitions);
            return this;
        }

        public Builder jobProperties(Properties jobProperties) {
            stepConfiguration.setJobProperties(jobProperties);
            return this;
        }

        public Builder engine(String engine) {
            stepConfiguration.setEngine(engine);
            return this;
        }

        public Builder dataflowExtraSources(Map<String, String> dataflowExtaSources) {
            stepConfiguration.setExtraSources(dataflowExtaSources);
            return this;
        }

        public Builder targetTableName(String targetTableName) {
            stepConfiguration.setTargetTableName(targetTableName);
            return this;
        }

        public Builder queue(String queue) {
            stepConfiguration.setQueue(queue);
            return this;
        }

        public Builder setBeanName(String string) {
            stepConfiguration.setBeanName("cascadingBulkMatchDataflow");
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            stepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public CascadingBulkMatchWorkflowConfiguration build() {
            configuration.setContainerConfiguration("cascadingBulkMatchWorkflow", customerSpace,
                    "CascadingBulkMatchWorkflow");
            configuration.add(stepConfiguration);
            return configuration;
        }

    }

}