package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.CatalogImport;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BuildCatalogStepConfiguration;

public class ProcessCatalogWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private ProcessCatalogWorkflowConfiguration configuration = new ProcessCatalogWorkflowConfiguration();
        private BuildCatalogStepConfiguration buildCatalogConfig = new BuildCatalogStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            buildCatalogConfig.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            buildCatalogConfig.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder entityMatchEnabled(boolean entityMatchEnabled) {
            buildCatalogConfig.setSkipStep(!entityMatchEnabled);
            buildCatalogConfig.setEntityMatchEnabled(entityMatchEnabled);
            return this;
        }

        public Builder catalogTables(Map<String, String> catalogTables) {
            buildCatalogConfig.setCatalogTables(catalogTables);
            return this;
        }

        public Builder catalogImports(Map<String, List<CatalogImport>> catalogImports) {
            buildCatalogConfig.setCatalogImports(catalogImports);
            return this;
        }

        public ProcessCatalogWorkflowConfiguration build() {
            configuration.setContainerConfiguration("processCatalogWorkflow",
                    configuration.getCustomerSpace(), configuration.getClass().getSimpleName());
            configuration.add(buildCatalogConfig);
            return configuration;
        }
    }
}
