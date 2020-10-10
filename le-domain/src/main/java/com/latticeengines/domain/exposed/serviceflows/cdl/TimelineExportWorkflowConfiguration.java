package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.TimelineExportRequest;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ExportTimelineSparkStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateTimelineExportUniverseStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportTimelineToS3StepConfiguration;

public class TimelineExportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private TimelineExportWorkflowConfiguration configuration = new TimelineExportWorkflowConfiguration();
        private GenerateTimelineExportUniverseStepConfiguration timelineUniverseStepConfiguration =
                new GenerateTimelineExportUniverseStepConfiguration();
        private ExportTimelineSparkStepConfiguration exportTimelineSparkStepConfiguration =
                new ExportTimelineSparkStepConfiguration();
        private ExportTimelineToS3StepConfiguration importExportS3 = new ExportTimelineToS3StepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            exportTimelineSparkStepConfiguration.setCustomer(customerSpace.toString());
            timelineUniverseStepConfiguration.setCustomerSpace(customerSpace);
            importExportS3.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            importExportS3.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            importExportS3.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder setSegment(MetadataSegment metadataSegment) {
            timelineUniverseStepConfiguration.setMetadataSegment(metadataSegment);
            return this;
        }

        public Builder setRequest(TimelineExportRequest request) {
            exportTimelineSparkStepConfiguration.setRequest(request);
            return this;
        }

        public Builder setTimelineTableNames(Map<String, String> timelineTableNames) {
            exportTimelineSparkStepConfiguration.setTimelineTableNames(timelineTableNames);
            return this;
        }

        public Builder setVersion(DataCollection.Version version) {
            timelineUniverseStepConfiguration.setVersion(version);
            return this;
        }

        public TimelineExportWorkflowConfiguration build() {
            configuration.setContainerConfiguration("timelineExportWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(exportTimelineSparkStepConfiguration);
            configuration.add(timelineUniverseStepConfiguration);
            configuration.add(importExportS3);
            return configuration;
        }
    }
}
