package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportListOfEntitiesConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class CDLImportWorkflowConfiguration extends WorkflowConfiguration {
    
    private String microServiceHostPort;
    
    public CDLImportWorkflowConfiguration() {
    }
    
    public String getMicroServiceHostPort() {
        return microServiceHostPort;
    }

    public void setMicroServiceHostPort(String microServiceHostPort) {
        this.microServiceHostPort = microServiceHostPort;
    }

    public static class Builder {

        private CDLImportWorkflowConfiguration configuration = new CDLImportWorkflowConfiguration();

        private ImportListOfEntitiesConfiguration.Builder builder = new ImportListOfEntitiesConfiguration.Builder();
        
        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("cdlCreateStagingTablesWorkflow", customerSpace, "cdlCreateStagingTablesWorkflow");
            return this;
        }
        
        public Builder microServiceHostPort(String microServiceHostPort) {
            configuration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder sourceFile(String entityName, SourceFile sourceFile) {
            builder.sourceFile(entityName, sourceFile);
            return this;
        }

        public CDLImportWorkflowConfiguration build() {
            ImportListOfEntitiesConfiguration config = builder.build();
            
            config.setCustomerSpace(configuration.getCustomerSpace());
            config.setInternalResourceHostPort(configuration.getInternalResourceHostPort());
            config.setMicroServiceHostPort(configuration.getMicroServiceHostPort());
            config.getImportConfigs().values().forEach(x -> x.setMicroServiceHostPort(configuration.getMicroServiceHostPort()));
            config.getImportConfigs().values().forEach(x -> x.setInternalResourceHostPort(configuration.getInternalResourceHostPort()));
            configuration.add(config);
            return configuration;
        }

    }
}
