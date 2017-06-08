package com.latticeengines.cdl.workflow.steps.stage;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.flows.cdl.CreateStagingTableParameters;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class StageListOfEntitiesConfiguration extends MicroserviceStepConfiguration {

    private Map<String, StageDataConfiguration> stageDataConfigurations = new HashMap<>();
    
    public Map<String, StageDataConfiguration> getStageDataConfigs() {
        return stageDataConfigurations;
    }

    public void setStageDataConfigs(Map<String, StageDataConfiguration> stageDataConfigs) {
        this.stageDataConfigurations = stageDataConfigs;
    }

    @Override
    public void setCustomerSpace(CustomerSpace customerSpace) {
        super.setCustomerSpace(customerSpace);
        if (stageDataConfigurations == null) {
            return;
        }
        stageDataConfigurations.values().stream().forEach(x -> x.setCustomerSpace(customerSpace));
    }

    @Override
    public void setMicroServiceHostPort(String microServiceHostPort) {
        super.setMicroServiceHostPort(microServiceHostPort);
        if (stageDataConfigurations == null) {
            return;
        }
        stageDataConfigurations.values().stream().forEach(x -> x.setMicroServiceHostPort(microServiceHostPort));
    }

    @Override
    public void setInternalResourceHostPort(String internalResourceHostPort) {
        super.setInternalResourceHostPort(internalResourceHostPort);
        if (stageDataConfigurations == null) {
            return;
        }
        stageDataConfigurations.values().stream().forEach(x -> x.setInternalResourceHostPort(internalResourceHostPort));
    }

    public static class Builder {
        
        private Map<String, StageDataConfiguration> stageDataConfigs = new HashMap<>();
        
        private StageDataConfiguration getStageConfigurationByName(String name) {
            StageDataConfiguration stageDataConfiguration = stageDataConfigs.get(name);
            if (stageDataConfiguration == null) {
                stageDataConfiguration = new StageDataConfiguration();
                stageDataConfigs.put(name, stageDataConfiguration);
            }
            return stageDataConfiguration;
        }
        
        public Builder sourceFile(String name, SourceFile sourceFile) {
            StageDataConfiguration config = getStageConfigurationByName(name);
            config.setSourceFile(sourceFile);
            config.setTargetTableName(sourceFile.getSchemaInterpretation().name() + "StagingNoLatticeId");
            CreateStagingTableParameters params = new CreateStagingTableParameters();
            params.setSourceFile(sourceFile);
            config.setDataFlowParams(params);
            return this;
        }
        
        public StageListOfEntitiesConfiguration build() {
            StageListOfEntitiesConfiguration config = new StageListOfEntitiesConfiguration();
            config.setStageDataConfigs(stageDataConfigs);
            return config;
        }
        
    }
}
