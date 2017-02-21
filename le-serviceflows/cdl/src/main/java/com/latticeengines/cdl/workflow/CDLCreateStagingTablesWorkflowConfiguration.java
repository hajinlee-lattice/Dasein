package com.latticeengines.cdl.workflow;

import com.latticeengines.cdl.workflow.steps.match.MatchListOfEntitiesConfiguration;
import com.latticeengines.cdl.workflow.steps.stage.StageListOfEntitiesConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class CDLCreateStagingTablesWorkflowConfiguration extends WorkflowConfiguration {
    
    private String microServiceHostPort;
    
    public CDLCreateStagingTablesWorkflowConfiguration() {
    }
    
    public String getMicroServiceHostPort() {
        return microServiceHostPort;
    }

    public void setMicroServiceHostPort(String microServiceHostPort) {
        this.microServiceHostPort = microServiceHostPort;
    }

    public static class Builder {

        private CDLCreateStagingTablesWorkflowConfiguration configuration = new CDLCreateStagingTablesWorkflowConfiguration();

        private StageListOfEntitiesConfiguration.Builder builder = new StageListOfEntitiesConfiguration.Builder();
        
        private MatchListOfEntitiesConfiguration.Builder matchBuilder = new MatchListOfEntitiesConfiguration.Builder();
        
        
        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("cdlCreateStagingTablesWorkflow", customerSpace, "cdlCreateStagingTablesWorkflow");
            matchBuilder.customer(customerSpace);
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
            matchBuilder.sourceFile(entityName, sourceFile);
            return this;
        }

        public CDLCreateStagingTablesWorkflowConfiguration build() {
            StageListOfEntitiesConfiguration stageConfig = builder.build();
            stageConfig.setCustomerSpace(configuration.getCustomerSpace());
            stageConfig.setInternalResourceHostPort(configuration.getInternalResourceHostPort());
            stageConfig.setMicroServiceHostPort(configuration.getMicroServiceHostPort());
            stageConfig.getStageDataConfigs().values().forEach(x -> x.setMicroServiceHostPort(configuration.getMicroServiceHostPort()));
            stageConfig.getStageDataConfigs().values().forEach(x -> x.setInternalResourceHostPort(configuration.getInternalResourceHostPort()));
            
            MatchListOfEntitiesConfiguration matchConfig = matchBuilder.build();
            matchConfig.setCustomerSpace(configuration.getCustomerSpace());
            matchConfig.setInternalResourceHostPort(configuration.getInternalResourceHostPort());
            matchConfig.setMicroServiceHostPort(configuration.getMicroServiceHostPort());
            matchConfig.getMatchConfigs().values().forEach(x -> x.setMicroServiceHostPort(configuration.getMicroServiceHostPort()));
            matchConfig.getMatchConfigs().values().forEach(x -> x.setInternalResourceHostPort(configuration.getInternalResourceHostPort()));
            
            configuration.add(stageConfig);
            configuration.add(matchConfig);
            
            return configuration;
        }

    }
}
