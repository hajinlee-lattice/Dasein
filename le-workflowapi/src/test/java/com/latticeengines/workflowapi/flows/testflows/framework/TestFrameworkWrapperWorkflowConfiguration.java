package com.latticeengines.workflowapi.flows.testflows.framework;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

// Configuration file for wrapper workflow that runs the step or workflow under test.
public class TestFrameworkWrapperWorkflowConfiguration extends WorkflowConfiguration {

    private static final Logger log = LoggerFactory.getLogger(TestFrameworkWrapperWorkflowConfiguration.class);

    @JsonProperty("testingSingleStep")
    private boolean testingSingleStep;

    @JsonProperty("preStepBeanName")
    private String preStepBeanName;

    // Either testStepBeanName or testWorkflowName is used depending on if the framework is testing a single Step or
    // a full Workflow, respectively.
    @JsonProperty("testStepBeanName")
    private String testStepBeanName;

    @JsonProperty("testWorkflowName")
    private String testWorkflowName;

    @JsonProperty("postStepBeanName")
    private String postStepBeanName;

    public boolean isTestingSingleStep() { return testingSingleStep; }

    public void setTestingSingleStep(boolean testingSingleStep) { this.testingSingleStep = testingSingleStep; }

    public String getPreStepBeanName() { return preStepBeanName; }

    public void setPreStepBeanName(String preStepBeanName) { this.preStepBeanName = preStepBeanName; }

    public String getTestStepBeanName() { return testStepBeanName; }

    public void setTestStepBeanName(String testStepBeanName) { this.testStepBeanName = testStepBeanName; }

    public String getTestWorkflowName() { return testWorkflowName; }

    public void setTestWorkflowName(String testWorkflowName) { this.testWorkflowName = testWorkflowName; }

    public String getPostStepBeanName() { return postStepBeanName; }

    public void setPostStepBeanName(String postStepBeanName) { this.postStepBeanName = postStepBeanName; }


    public static class Builder {
        private TestFrameworkWrapperWorkflowConfiguration configuration = new TestFrameworkWrapperWorkflowConfiguration();

        // True if the framework is being used to
        private boolean testingSingleStep;

        private TestBasePreprocessingStepConfiguration preStepConfig;
        // Either testStepConfig or testWorkflowConfig is used depending on if the framework is testing a single Step
        // or full Workflow, respectively.
        private BaseStepConfiguration testStepConfig;
        private WorkflowConfiguration testWorkflowConfig;
        private TestBasePostprocessingStepConfiguration postStepConfig;

        public Builder(boolean testingSingleStep) {
            this.testingSingleStep = testingSingleStep;
            configuration.setTestingSingleStep(testingSingleStep);
        }

        public Builder setTestPreprocessStepConfiguration(TestBasePreprocessingStepConfiguration preStepConfig) {
            this.preStepConfig = preStepConfig;
            configuration.setPreStepBeanName(preStepConfig.getStepBeanName());
            return this;
        }

        // This function is used to set up a test of a single Step and should not be called when testing a full
        // Workflow.
        public Builder setTestStepBeanName(String testStepBeanName) {
            assert(testingSingleStep);
            configuration.setTestStepBeanName(testStepBeanName);
            configuration.setTestWorkflowName(null);
            return this;
        }

        // This function is used to set up a test of a single Step and should not be called when testing a full
        // Workflow.
        public Builder setTestStepConfiguration(BaseStepConfiguration testStepConfig) {
            assert(testingSingleStep);
            this.testStepConfig = testStepConfig;
            return this;
        }

        // This function is used to set up a test of a Workflow and should not be called when testing only a Step.
        public Builder setTestWorkflowName(String testWorkflowName) {
            assert(!testingSingleStep);
            configuration.setTestStepBeanName(null);
            configuration.setTestWorkflowName(testWorkflowName);
            return this;
        }

        // This function is used to set up a test of a Workflow and should not be called when testing only a Step.
        public Builder setTestWorkflowConfiguration(WorkflowConfiguration workflowConfiguration) {
            assert(!testingSingleStep);
            testWorkflowConfig = workflowConfiguration;
            return this;
        }

        public Builder setTestPostprocessStepConfiguration(TestBasePostprocessingStepConfiguration postStepConfig) {
            this.postStepConfig = postStepConfig;
            configuration.setPostStepBeanName(postStepConfig.getStepBeanName());
            return this;
        }


        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("testFrameworkWrapperWorkflow", customerSpace,
                    "testFrameworkWrapperWorkflow");
            preStepConfig.setCustomerSpace(customerSpace.toString());
            postStepConfig.setCustomerSpace(customerSpace.toString());
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder workflowContainerMem(int mb) {
            configuration.setContainerMemoryMB(mb);
            return this;
        }

        public TestFrameworkWrapperWorkflowConfiguration build() {
            configuration.add(preStepConfig);
            if (testingSingleStep) {
                configuration.add(testStepConfig);
            } else {
                configuration.add(testWorkflowConfig);
            }
            configuration.add(postStepConfig);
            return configuration;
        }
    }
}
