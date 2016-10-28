package com.latticeengines.cdl.workflow;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;
import com.latticeengines.serviceflows.workflow.importdata.ImportStepConfiguration;

public class ImportListOfEntitiesConfiguration extends MicroserviceStepConfiguration {

    private Map<String, ImportStepConfiguration> importConfigs;
    
    protected ImportListOfEntitiesConfiguration() {}
    
    public Map<String, ImportStepConfiguration> getImportConfigs() {
        return importConfigs;
    }

    public void setImportConfigs(Map<String, ImportStepConfiguration> importConfigs) {
        this.importConfigs = importConfigs;
    }

    @Override
    public void setCustomerSpace(CustomerSpace customerSpace) {
        super.setCustomerSpace(customerSpace);
        if (importConfigs == null) {
            return;
        }
        for (Map.Entry<String, ImportStepConfiguration> entry : importConfigs.entrySet()) {
            entry.getValue().setCustomerSpace(customerSpace);
        }
    }

    @Override
    public void setMicroServiceHostPort(String microServiceHostPort) {
        super.setMicroServiceHostPort(microServiceHostPort);
        if (importConfigs == null) {
            return;
        }
        for (Map.Entry<String, ImportStepConfiguration> entry : importConfigs.entrySet()) {
            entry.getValue().setMicroServiceHostPort(microServiceHostPort);
        }
    }

    @Override
    public void setInternalResourceHostPort(String internalResourceHostPort) {
        super.setInternalResourceHostPort(internalResourceHostPort);
        if (importConfigs == null) {
            return;
        }
        for (Map.Entry<String, ImportStepConfiguration> entry : importConfigs.entrySet()) {
            entry.getValue().setInternalResourceHostPort(internalResourceHostPort);
        }
    }

    public abstract static class Builder {
        
        private Map<String, ImportStepConfiguration> importConfigs = new HashMap<>();
        
        private ImportStepConfiguration getImportConfigurationByName(String name) {
            ImportStepConfiguration importConfiguration = importConfigs.get(name);
            if (importConfiguration == null) {
                importConfiguration = new ImportStepConfiguration();
                importConfigs.put(name, importConfiguration);
            }
            return importConfiguration;
        }
        
        public Builder customer(CustomerSpace customerSpace) {
            return this;
        }
        
        public Builder sourceType(String name, SourceType sourceType) {
            ImportStepConfiguration config = getImportConfigurationByName(name);
            config.setSourceType(sourceType);
            return this;
        }
        
        public Builder sourceFileName(String name, String sourceFileName) {
            ImportStepConfiguration config = getImportConfigurationByName(name);
            config.setSourceFileName(sourceFileName);
            return this;
        }
        
        @SuppressWarnings("unchecked")
        public <T extends ImportListOfEntitiesConfiguration> T build() {
            ImportListOfEntitiesConfiguration config = getConfiguration();
            config.setImportConfigs(importConfigs);
            return (T) config;
        }
        
        public abstract <T extends ImportListOfEntitiesConfiguration> T getConfiguration();
    }
}
