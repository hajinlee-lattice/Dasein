package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportStepConfiguration;

public class ImportAndCreateDiffWorkflowConfiguration extends WorkflowConfiguration {

    private ImportAndCreateDiffWorkflowConfiguration() {
    }

    public static class Builder {
        private ImportAndCreateDiffWorkflowConfiguration configuration = new ImportAndCreateDiffWorkflowConfiguration();

        private ImportStepConfiguration accountImportConfiguration = new ImportStepConfiguration();
        private ImportStepConfiguration contactImportConfiguration = new ImportStepConfiguration();
        private Map<String, ImportStepConfiguration> timeSeriesImportConfigurations = new HashMap<>();
        private Map<String, ImportStepConfiguration> categoryImportConfigurations = new HashMap<>();
        
        
        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("importAndCreateDiffWorkflow", customerSpace,
                    "importAndCreateDiffWorkflow");
            accountImportConfiguration.setCustomerSpace(customerSpace);
            contactImportConfiguration.setCustomerSpace(customerSpace);
            for (Map.Entry<String, ImportStepConfiguration> entry : timeSeriesImportConfigurations.entrySet()) {
                entry.getValue().setCustomerSpace(customerSpace);
            }
            for (Map.Entry<String, ImportStepConfiguration> entry : categoryImportConfigurations.entrySet()) {
                entry.getValue().setCustomerSpace(customerSpace);
            }
            return this;
        }
        
        private ImportStepConfiguration getTimeSeriesImportConfigurationByName(String name) {
            ImportStepConfiguration timeSeriesImportConfiguration = timeSeriesImportConfigurations.get(name);
            if (timeSeriesImportConfiguration == null) {
                timeSeriesImportConfiguration = new ImportStepConfiguration();
                timeSeriesImportConfigurations.put(name, timeSeriesImportConfiguration);
            }
            return timeSeriesImportConfiguration;
        }
        
        private ImportStepConfiguration getCategoryImportConfigurationByName(String name) {
            ImportStepConfiguration categoryImportConfiguration = categoryImportConfigurations.get(name);
            if (categoryImportConfiguration == null) {
                categoryImportConfiguration = new ImportStepConfiguration();
                categoryImportConfigurations.put(name, categoryImportConfiguration);
            }
            return categoryImportConfiguration;
        }
        
        public Builder timeSeriesSourceFileName(String timeSeriesName, String sourceFileName) {
            ImportStepConfiguration config = getTimeSeriesImportConfigurationByName(timeSeriesName);
            config.setSourceFileName(sourceFileName);
            return this;
        }

        public Builder timeSeriesSourceType(String timeSeriesName, SourceType sourceType) {
            ImportStepConfiguration config = getTimeSeriesImportConfigurationByName(timeSeriesName);
            config.setSourceType(sourceType);
            return this;
        }

        public Builder categorySourceFileName(String category, String sourceFileName) {
            ImportStepConfiguration config = getCategoryImportConfigurationByName(category);
            config.setSourceFileName(sourceFileName);
            return this;
        }

        public Builder categorySourceType(String category, SourceType sourceType) {
            ImportStepConfiguration config = getCategoryImportConfigurationByName(category);
            config.setSourceType(sourceType);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            accountImportConfiguration.setMicroServiceHostPort(microServiceHostPort);
            contactImportConfiguration.setMicroServiceHostPort(microServiceHostPort);
            for (Map.Entry<String, ImportStepConfiguration> entry : timeSeriesImportConfigurations.entrySet()) {
                entry.getValue().setMicroServiceHostPort(microServiceHostPort);
            }
            for (Map.Entry<String, ImportStepConfiguration> entry : categoryImportConfigurations.entrySet()) {
                entry.getValue().setMicroServiceHostPort(microServiceHostPort);
            }
            return this;
        }

        public Builder accountSourceFileName(String sourceFileName) {
            accountImportConfiguration.setSourceFileName(sourceFileName);
            return this;
        }

        public Builder accountSourceType(SourceType sourceType) {
            accountImportConfiguration.setSourceType(sourceType);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            accountImportConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder inputTableName(String tableName) {
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public ImportAndCreateDiffWorkflowConfiguration build() {
            configuration.add(accountImportConfiguration);
            configuration.add(contactImportConfiguration);
            for (Map.Entry<String, ImportStepConfiguration> entry : timeSeriesImportConfigurations.entrySet()) {
                configuration.add(entry.getValue());
            }
            for (Map.Entry<String, ImportStepConfiguration> entry : categoryImportConfigurations.entrySet()) {
                configuration.add(entry.getValue());
            }
            
            return configuration;
        }

    }
}
