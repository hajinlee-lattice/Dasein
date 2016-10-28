package com.latticeengines.cdl.workflow;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class CDLImportWorkflowConfiguration extends WorkflowConfiguration {
    
    private String microServiceHostPort;

    private CDLImportWorkflowConfiguration() {
    }

    public String getMicroServiceHostPort() {
        return microServiceHostPort;
    }

    public void setMicroServiceHostPort(String microServiceHostPort) {
        this.microServiceHostPort = microServiceHostPort;
    }

    public static class Builder {
        private CDLImportWorkflowConfiguration configuration = new CDLImportWorkflowConfiguration();

        private ImportAccountStepConfiguration accountImportConfiguration = new ImportAccountStepConfiguration();
        private ImportContactStepConfiguration contactImportConfiguration = new ImportContactStepConfiguration();
        private ImportTimeSeriesConfiguration.Builder timeSeriesBuilder = new ImportTimeSeriesConfiguration.Builder();
        private ImportCategoryConfiguration.Builder categoryBuilder = new ImportCategoryConfiguration.Builder();
        
        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("cdlImportWorkflow", customerSpace, "cdlImportWorkflow");
            accountImportConfiguration.setCustomerSpace(customerSpace);
            contactImportConfiguration.setCustomerSpace(customerSpace);
            return this;
        }
        
        public Builder timeSeriesSourceFileName(String timeSeriesName, String sourceFileName) {
            timeSeriesBuilder.sourceFileName(timeSeriesName, sourceFileName);
            return this;
        }

        public Builder timeSeriesSourceType(String timeSeriesName, SourceType sourceType) {
            timeSeriesBuilder.sourceType(timeSeriesName, sourceType);
            return this;
        }

        public Builder categorySourceFileName(String category, String sourceFileName) {
            categoryBuilder.sourceFileName(category, sourceFileName);
            return this;
        }

        public Builder categorySourceType(String category, SourceType sourceType) {
            categoryBuilder.sourceType(category, sourceType);
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

        public Builder contactSourceFileName(String sourceFileName) {
            contactImportConfiguration.setSourceFileName(sourceFileName);
            return this;
        }

        public Builder contactSourceType(SourceType sourceType) {
            contactImportConfiguration.setSourceType(sourceType);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            accountImportConfiguration.setMicroServiceHostPort(microServiceHostPort);
            contactImportConfiguration.setMicroServiceHostPort(microServiceHostPort);
            configuration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            accountImportConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            contactImportConfiguration.setInternalResourceHostPort(internalResourceHostPort);
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

        public CDLImportWorkflowConfiguration build() {
            configuration.add(accountImportConfiguration);
            configuration.add(contactImportConfiguration);
            
            ImportTimeSeriesConfiguration timeSeriesConfig = timeSeriesBuilder.build();
            ImportCategoryConfiguration categoryConfig = categoryBuilder.build();
            
            timeSeriesConfig.setCustomerSpace(configuration.getCustomerSpace());
            timeSeriesConfig.setInternalResourceHostPort(configuration.getInternalResourceHostPort());
            timeSeriesConfig.setMicroServiceHostPort(configuration.microServiceHostPort);

            categoryConfig.setCustomerSpace(configuration.getCustomerSpace());
            categoryConfig.setInternalResourceHostPort(configuration.getInternalResourceHostPort());
            categoryConfig.setMicroServiceHostPort(configuration.microServiceHostPort);
            
            configuration.add(timeSeriesConfig);
            configuration.add(categoryConfig);
            
            return configuration;
        }

    }
}
