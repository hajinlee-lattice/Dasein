package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BuildCatalogStepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class ProcessCatalogWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return Arrays.asList(SoftwareLibrary.Scoring.getName(), SoftwareLibrary.CDL.getName());
    }

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

        public Builder catalogIngestionBehaviors(Map<String, DataFeedTask.IngestionBehavior> ingestionBehaviors) {
            buildCatalogConfig.setIngestionBehaviors(ingestionBehaviors);
            return this;
        }

        public Builder catalogPrimaryKeyColumns(Map<String, String> primaryKeyColumns) {
            buildCatalogConfig.setPrimaryKeyColumns(primaryKeyColumns);
            return this;
        }

        public Builder catalogImports(Map<String, List<ActivityImport>> catalogImports) {
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
