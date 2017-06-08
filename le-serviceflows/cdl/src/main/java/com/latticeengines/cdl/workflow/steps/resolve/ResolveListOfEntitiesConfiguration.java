package com.latticeengines.cdl.workflow.steps.resolve;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.flows.cdl.ResolveStagingAndRuntimeTableParameters;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ResolveListOfEntitiesConfiguration extends MicroserviceStepConfiguration {

    private Map<String, ResolveDataConfiguration> resolveDataConfigurations = new HashMap<>();
    
    public Map<String, ResolveDataConfiguration> getResolveDataConfigs() {
        return resolveDataConfigurations;
    }

    public void setResolveDataConfigs(Map<String, ResolveDataConfiguration> resolveDataConfigs) {
        this.resolveDataConfigurations = resolveDataConfigs;
    }

    @Override
    public void setCustomerSpace(CustomerSpace customerSpace) {
        super.setCustomerSpace(customerSpace);
        if (resolveDataConfigurations == null) {
            return;
        }
        resolveDataConfigurations.values().stream().forEach(x -> x.setCustomerSpace(customerSpace));
    }

    @Override
    public void setMicroServiceHostPort(String microServiceHostPort) {
        super.setMicroServiceHostPort(microServiceHostPort);
        if (resolveDataConfigurations == null) {
            return;
        }
        resolveDataConfigurations.values().stream().forEach(x -> x.setMicroServiceHostPort(microServiceHostPort));
    }

    @Override
    public void setInternalResourceHostPort(String internalResourceHostPort) {
        super.setInternalResourceHostPort(internalResourceHostPort);
        if (resolveDataConfigurations == null) {
            return;
        }
        resolveDataConfigurations.values().stream().forEach(x -> x.setInternalResourceHostPort(internalResourceHostPort));
    }

    public static class Builder {
        
        private Map<String, ResolveDataConfiguration> resolveDataConfigs = new HashMap<>();
        
        private ResolveDataConfiguration getResolveDataConfigurationByName(String name) {
            ResolveDataConfiguration resolveDataConfiguration = resolveDataConfigs.get(name);
            if (resolveDataConfiguration == null) {
                resolveDataConfiguration = new ResolveDataConfiguration();
                resolveDataConfigs.put(name, resolveDataConfiguration);
            }
            return resolveDataConfiguration;
        }
        
        public Builder sourceFile(String name, SourceFile sourceFile) {
            ResolveDataConfiguration config = getResolveDataConfigurationByName(name);
            config.setSourceFile(sourceFile);
            config.setTargetTableName(sourceFile.getSchemaInterpretation().name() + "StagingNoLatticeId");
            ResolveStagingAndRuntimeTableParameters params = new ResolveStagingAndRuntimeTableParameters();
            config.setDataFlowParams(params);
            return this;
        }
        
        public ResolveListOfEntitiesConfiguration build() {
            ResolveListOfEntitiesConfiguration config = new ResolveListOfEntitiesConfiguration();
            config.setResolveDataConfigs(resolveDataConfigs);
            return config;
        }
        
    }
}
