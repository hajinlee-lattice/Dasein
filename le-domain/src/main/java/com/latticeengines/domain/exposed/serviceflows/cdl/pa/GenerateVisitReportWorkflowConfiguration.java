package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.EnrichWebVisitStepConfiguration;

public class GenerateVisitReportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {

        private GenerateVisitReportWorkflowConfiguration configuration = new GenerateVisitReportWorkflowConfiguration();
        private EnrichWebVisitStepConfiguration enrichWebVisitSparkStepConfiguration =
                new EnrichWebVisitStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            enrichWebVisitSparkStepConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            enrichWebVisitSparkStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder activityStreamImports(Map<String, List<ActivityImport>> activityStreamImports) {
            enrichWebVisitSparkStepConfiguration.setStreamImports(activityStreamImports);
            return this;
        }

        public Builder activityStreams(Map<String, AtlasStream> activityStreams) {
            enrichWebVisitSparkStepConfiguration.setActivityStreamMap(activityStreams);
            return this;
        }

        public Builder setReplaceMode(boolean isReplaceMode) {
            enrichWebVisitSparkStepConfiguration.setReplaceMode(isReplaceMode);
            return this;
        }

        public Builder setRematchMode(boolean isRematchMode) {
            enrichWebVisitSparkStepConfiguration.setRematchMode(isRematchMode);
            return this;
        }

        public Builder setRebuildMode(boolean isRebuildMode) {
            enrichWebVisitSparkStepConfiguration.setRebuildMode(isRebuildMode);
            return this;
        }

        public Builder setCatalogImports(Map<String, List<ActivityImport>> catalogImports) {
            enrichWebVisitSparkStepConfiguration.setCatalogImports(catalogImports);
            return this;
        }

        public Builder setCatalog(List<Catalog> catalogs) {
            enrichWebVisitSparkStepConfiguration.setCatalogs(catalogs);
            return this;
        }

        public GenerateVisitReportWorkflowConfiguration build() {
            configuration.setContainerConfiguration("generateVisitReportWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(enrichWebVisitSparkStepConfiguration);
            return configuration;
        }
    }
}
