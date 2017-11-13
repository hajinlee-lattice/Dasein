package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.SegmentExportStepConfiguration;

public class SegmentExportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {
        private SegmentExportWorkflowConfiguration configuration = new SegmentExportWorkflowConfiguration();
        private SegmentExportStepConfiguration initStepConf = new SegmentExportStepConfiguration();
        private ExportStepConfiguration avroToCsvExportStepConf = new ExportStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("segmentExportWorkflow", customerSpace, "segmentExportWorkflow");
            initStepConf.setCustomerSpace(customerSpace);
            avroToCsvExportStepConf.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder metadataSegmentExportId(String metadataSegmentExportId) {
            initStepConf.setMetadataSegmentExportId(metadataSegmentExportId);
            return this;
        }

        public Builder targetPath(String targetPath) {
            avroToCsvExportStepConf.setExportTargetPath(targetPath);
            return this;
        }

        public Builder podId(String podId) {
            avroToCsvExportStepConf.setPodId(podId);
            return this;
        }

        public Builder exportInputPath(String exportInputPath) {
            avroToCsvExportStepConf.setExportInputPath(exportInputPath);
            return this;
        }

        public Builder tableName(String tableName) {
            avroToCsvExportStepConf.setTableName(tableName);
            return this;
        }

        public Builder workflow(String workflowName) {
            configuration.setWorkflowName(workflowName);
            configuration.setName(workflowName);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            avroToCsvExportStepConf.setProperties(inputProperties);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            avroToCsvExportStepConf.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            avroToCsvExportStepConf.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public SegmentExportWorkflowConfiguration build() {
            avroToCsvExportStepConf.setExportFormat(ExportFormat.CSV);
            avroToCsvExportStepConf.setExportDestination(ExportDestination.FILE);
            avroToCsvExportStepConf.setUsingDisplayName(true);

            configuration.add(initStepConf);
            configuration.add(avroToCsvExportStepConf);
            return configuration;
        }

    }
}
