package com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ImportListOfEntitiesConfiguration extends MicroserviceStepConfiguration {

    private Map<String, ImportSourceDataConfiguration> importConfigurations = new HashMap<>();
    private SourceFile sourceFile;
    
    public Map<String, ImportSourceDataConfiguration> getImportConfigs() {
        return importConfigurations;
    }

    public void setImportConfigs(Map<String, ImportSourceDataConfiguration> importConfigurations) {
        this.importConfigurations = importConfigurations;
    }

    @Override
    public void setCustomerSpace(CustomerSpace customerSpace) {
        super.setCustomerSpace(customerSpace);
        if (importConfigurations == null) {
            return;
        }
        importConfigurations.values().stream().forEach(x -> x.setCustomerSpace(customerSpace));
    }

    @Override
    public void setMicroServiceHostPort(String microServiceHostPort) {
        super.setMicroServiceHostPort(microServiceHostPort);
        if (importConfigurations == null) {
            return;
        }
        importConfigurations.values().stream().forEach(x -> x.setMicroServiceHostPort(microServiceHostPort));
    }

    @Override
    public void setInternalResourceHostPort(String internalResourceHostPort) {
        super.setInternalResourceHostPort(internalResourceHostPort);
        if (importConfigurations == null) {
            return;
        }
        importConfigurations.values().stream().forEach(x -> x.setInternalResourceHostPort(internalResourceHostPort));
    }

    public SourceFile getSourceFile() {
        return sourceFile;
    }

    public void setSourceFile(SourceFile sourceFile) {
        this.sourceFile = sourceFile;
    }

    public static class Builder {
        
        private Map<String, ImportSourceDataConfiguration> importConfigs = new HashMap<>();
        
        private ImportSourceDataConfiguration getStageConfigurationByName(String name) {
            ImportSourceDataConfiguration importConfig = importConfigs.get(name);
            if (importConfig == null) {
                importConfig = new ImportSourceDataConfiguration();
                importConfigs.put(name, importConfig);
            }
            return importConfig;
        }
        
        public Builder sourceFile(String name, SourceFile sourceFile) {
            ImportSourceDataConfiguration config = getStageConfigurationByName(name);
            config.setSourceFile(sourceFile);
            return this;
        }
        
        public ImportListOfEntitiesConfiguration build() {
            ImportListOfEntitiesConfiguration config = new ImportListOfEntitiesConfiguration();
            importConfigs.values().stream().forEach(x -> x.setSourceType(SourceType.FILE));
            importConfigs.values().stream().forEach(x -> x.setSourceFileName(x.getSourceFile().getName()));
            config.setImportConfigs(importConfigs);
            
            return config;
        }
        
    }
}
