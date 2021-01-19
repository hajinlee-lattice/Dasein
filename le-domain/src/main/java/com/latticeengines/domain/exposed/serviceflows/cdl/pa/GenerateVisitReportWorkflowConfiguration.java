package com.latticeengines.domain.exposed.serviceflows.cdl.pa;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.BaseCDLWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.EnrichWebVisitSparkStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;

public class GenerateVisitReportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {

        private GenerateVisitReportWorkflowConfiguration configuration = new GenerateVisitReportWorkflowConfiguration();
        private ProcessActivityStreamStepConfiguration processActivityStreamStepConfiguration =
                new ProcessActivityStreamStepConfiguration();
        private EnrichWebVisitSparkStepConfiguration enrichWebVisitSparkStepConfiguration =
                new EnrichWebVisitSparkStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            processActivityStreamStepConfiguration.setCustomerSpace(customerSpace);
            enrichWebVisitSparkStepConfiguration.setCustomer(customerSpace.toString());
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            processActivityStreamStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            enrichWebVisitSparkStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder activityStreamImports(Map<String, List<ActivityImport>> activityStreamImports) {
            processActivityStreamStepConfiguration.setStreamImports(activityStreamImports);
            return this;
        }

        public Builder activityStreams(Map<String, AtlasStream> activityStreams) {
            processActivityStreamStepConfiguration.setActivityStreamMap(activityStreams);
            enrichWebVisitSparkStepConfiguration.setActivityStreamMap(activityStreams);
            return this;
        }

        public Builder setReplaceMode(boolean isReplaceMode) {
            processActivityStreamStepConfiguration.setReplaceMode(isReplaceMode);
            enrichWebVisitSparkStepConfiguration.setReplaceMode(isReplaceMode);
            return this;
        }

        public Builder setRematchMode(boolean isRematchMode) {
            processActivityStreamStepConfiguration.setRematchMode(isRematchMode);
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

        public Builder entityMatchConfiguration(EntityMatchConfiguration configuration) {
            processActivityStreamStepConfiguration.setEntityMatchConfiguration(configuration);
            return this;
        }

        public Builder setCatalog(List<Catalog> catalogs) {
            enrichWebVisitSparkStepConfiguration.setCatalogs(catalogs);
            return this;
        }

        public GenerateVisitReportWorkflowConfiguration build() {
            configuration.setContainerConfiguration("generateVisitReportWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(processActivityStreamStepConfiguration);
            configuration.add(enrichWebVisitSparkStepConfiguration);
            return configuration;
        }
    }
}
