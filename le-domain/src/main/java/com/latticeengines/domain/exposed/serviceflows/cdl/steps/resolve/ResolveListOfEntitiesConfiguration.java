package com.latticeengines.domain.exposed.serviceflows.cdl.steps.resolve;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.ResolveStagingAndRuntimeTableParameters;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ResolveListOfEntitiesConfiguration extends MicroserviceStepConfiguration {

    private Map<String, ResolveDataConfiguration> resolveDataConfigurations = new HashMap<>();
    
    public Map<String, ResolveDataConfiguration> getResolveDataConfigs() {
        return resolveDataConfigurations;
    }

    void setResolveDataConfigs(Map<String, ResolveDataConfiguration> resolveDataConfigs) {
        this.resolveDataConfigurations = resolveDataConfigs;
    }

    @Override
    public void setCustomerSpace(CustomerSpace customerSpace) {
        super.setCustomerSpace(customerSpace);
        if (resolveDataConfigurations == null) {
            return;
        }
        resolveDataConfigurations.values().forEach(x -> x.setCustomerSpace(customerSpace));
    }

    @Override
    public void setMicroServiceHostPort(String microServiceHostPort) {
        super.setMicroServiceHostPort(microServiceHostPort);
        if (resolveDataConfigurations == null) {
            return;
        }
        resolveDataConfigurations.values().forEach(x -> x.setMicroServiceHostPort(microServiceHostPort));
    }

    @Override
    public void setInternalResourceHostPort(String internalResourceHostPort) {
        super.setInternalResourceHostPort(internalResourceHostPort);
        if (resolveDataConfigurations == null) {
            return;
        }
        resolveDataConfigurations.values().forEach(x -> x.setInternalResourceHostPort(internalResourceHostPort));
    }

    public static class Builder {
        
        private Map<String, ResolveDataConfiguration> resolveDataConfigs = new HashMap<>();
        
        private ResolveDataConfiguration getResolveDataConfigurationByName(String name) {
            return resolveDataConfigs.computeIfAbsent(name, k -> new ResolveDataConfiguration());
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
