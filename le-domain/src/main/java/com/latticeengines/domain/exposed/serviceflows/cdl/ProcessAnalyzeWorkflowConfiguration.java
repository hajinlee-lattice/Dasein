package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CombineStatisticsConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;

public class ProcessAnalyzeWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    private ProcessAnalyzeWorkflowConfiguration() {

    }

    public static class Builder {

        public ProcessAnalyzeWorkflowConfiguration configuration = new ProcessAnalyzeWorkflowConfiguration();

        private ProcessStepConfiguration processStepConfiguration = new ProcessStepConfiguration();
        private ProcessAccountStepConfiguration processAccountStepConfiguration = new ProcessAccountStepConfiguration();
        private ProcessContactStepConfiguration processContactStepConfiguration = new ProcessContactStepConfiguration();
        private ProcessProductStepConfiguration processProductStepConfiguration = new ProcessProductStepConfiguration();
        private ProcessTransactionStepConfiguration processTransactionStepConfiguration = new ProcessTransactionStepConfiguration();
        private CombineStatisticsConfiguration combineStatisticsConfiguration = new CombineStatisticsConfiguration();
        private RedshiftPublishWorkflowConfiguration.Builder redshiftPublishWorkflowConfigurationBuilder = new RedshiftPublishWorkflowConfiguration.Builder();

        public Builder initialDataFeedStatus(DataFeed.Status initialDataFeedStatus) {
            processStepConfiguration.setInitialDataFeedStatus(initialDataFeedStatus);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("processAnalyzeWorkflow", customerSpace,
                    "processAnalyzeWorkflow");
            processStepConfiguration.setCustomerSpace(customerSpace);
            processAccountStepConfiguration.setCustomerSpace(customerSpace);
            processContactStepConfiguration.setCustomerSpace(customerSpace);
            processProductStepConfiguration.setCustomerSpace(customerSpace);
            processTransactionStepConfiguration.setCustomerSpace(customerSpace);
            combineStatisticsConfiguration.setCustomerSpace(customerSpace);
            redshiftPublishWorkflowConfigurationBuilder.customer(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            processStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            redshiftPublishWorkflowConfigurationBuilder.microServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            processStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processAccountStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processContactStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processProductStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processTransactionStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            redshiftPublishWorkflowConfigurationBuilder.internalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder hdfsToRedshiftConfiguration(HdfsToRedshiftConfiguration hdfsToRedshiftConfiguration) {
            redshiftPublishWorkflowConfigurationBuilder.hdfsToRedshiftConfiguration(hdfsToRedshiftConfiguration);
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

        public Builder importJobIds(List<Long> importJobIds) {
            processStepConfiguration.setImportJobIds(importJobIds);
            return this;
        }

        public ProcessAnalyzeWorkflowConfiguration build() {
            configuration.add(processStepConfiguration);
            configuration.add(processAccountStepConfiguration);
            configuration.add(processContactStepConfiguration);
            configuration.add(processProductStepConfiguration);
            configuration.add(processTransactionStepConfiguration);
            configuration.add(combineStatisticsConfiguration);
            configuration.add(redshiftPublishWorkflowConfigurationBuilder.build());
            return configuration;
        }
    }
}
