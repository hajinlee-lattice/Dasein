package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;

public class MatchEntityWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private MatchEntityWorkflowConfiguration configuration = new MatchEntityWorkflowConfiguration();

        private ProcessAccountStepConfiguration processAccountStepConfiguration = new ProcessAccountStepConfiguration();
        private ProcessContactStepConfiguration processContactStepConfiguration = new ProcessContactStepConfiguration();
        private ProcessTransactionStepConfiguration processTxnStepConfiguration = new ProcessTransactionStepConfiguration();
        private ProcessActivityStreamStepConfiguration processActivityStreamStepConfiguration = new ProcessActivityStreamStepConfiguration();
        private ProcessStepConfiguration processStepConfiguration = new ProcessStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processAccountStepConfiguration.setCustomerSpace(customerSpace);
            processContactStepConfiguration.setCustomerSpace(customerSpace);
            processTxnStepConfiguration.setCustomerSpace(customerSpace);
            processActivityStreamStepConfiguration.setCustomerSpace(customerSpace);
            processStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            processAccountStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processContactStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processTxnStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processActivityStreamStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            processStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder dataCloudVersion(DataCloudVersion dataCloudVersion) {
            processAccountStepConfiguration.setDataCloudVersion(dataCloudVersion.getVersion());
            return this;
        }

        public Builder systemIdMap(Map<String, List<String>> systemIds) {
            processAccountStepConfiguration.setSystemIdMap(systemIds);
            processContactStepConfiguration.setSystemIdMap(systemIds);
            processTxnStepConfiguration.setSystemIdMap(systemIds);
            processActivityStreamStepConfiguration.setSystemIdMap(systemIds);
            return this;
        }

        public Builder defaultSystemIdMap(Map<String, String> defaultSystemIds) {
            processAccountStepConfiguration.setDefaultSystemIdMap(defaultSystemIds);
            processContactStepConfiguration.setDefaultSystemIdMap(defaultSystemIds);
            processTxnStepConfiguration.setDefaultSystemIdMap(defaultSystemIds);
            processActivityStreamStepConfiguration.setDefaultSystemIdMap(defaultSystemIds);
            return this;
        }

        public Builder entityMatchEnabled(boolean entityMatchEnabled) {
            processAccountStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            processContactStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            processTxnStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            processStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            processActivityStreamStepConfiguration.setEntityMatchEnabled(entityMatchEnabled);
            processActivityStreamStepConfiguration.setSkipStep(!entityMatchEnabled);
            return this;
        }

        public Builder entityMatchGAOnly(boolean gaOnly) {
            processContactStepConfiguration.setEntityMatchGAOnly(gaOnly);
            processTxnStepConfiguration.setEntityMatchGAOnly(gaOnly);
            processActivityStreamStepConfiguration.setEntityMatchGAOnly(gaOnly);
            return this;
        }

        public Builder activeRawStreamTables(Map<String, String> rawStreamTables) {
            processActivityStreamStepConfiguration.setActiveRawStreamTables(rawStreamTables);
            return this;
        }

        public Builder activityStreams(Map<String, AtlasStream> activityStreams) {
            processActivityStreamStepConfiguration.setActivityStreamMap(activityStreams);
            return this;
        }

        public Builder activityStreamImports(Map<String, List<ActivityImport>> activityStreamImports) {
            processActivityStreamStepConfiguration.setStreamImports(activityStreamImports);
            return this;
        }

        public Builder setReplaceMode(boolean isReplaceMode) {
            processActivityStreamStepConfiguration.setReplaceMode(isReplaceMode);
            return this;
        }

        public Builder setRematchMode(boolean isRematchMode) {
            processActivityStreamStepConfiguration.setRematchMode(isRematchMode);
            return this;
        }

        public MatchEntityWorkflowConfiguration build() {
            configuration.setContainerConfiguration("matchEntityWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(processAccountStepConfiguration);
            configuration.add(processContactStepConfiguration);
            configuration.add(processTxnStepConfiguration);
            configuration.add(processStepConfiguration);
            configuration.add(processActivityStreamStepConfiguration);
            return configuration;
        }
    }
}
