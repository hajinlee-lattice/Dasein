package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.SegmentExportStepConfiguration;

public class SegmentExportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String EXPORT_OUTPUT_PATH = "EXPORT_OUTPUT_PATH";
    public static final String EXPORT_INPUT_PATH = "EXPORT_INPUT_PATH";
    public static final String SEGMENT_EXPORT_ID = "SEGMENT_EXPORT_ID";
    public static final String SEGMENT_DISPLAY_NAME = "SEGMENT_DISPLAY_NAME";
    public static final String EXPORT_OBJECT_TYPE = "EXPORT_OBJECT_TYPE";
    public static final String EXPIRE_BY_UTC_TIMESTAMP = "EXPIRE_BY_UTC_TIMESTAMP";

    public static class Builder {
        private SegmentExportWorkflowConfiguration configuration = new SegmentExportWorkflowConfiguration();
        private SegmentExportStepConfiguration initStepConf = new SegmentExportStepConfiguration();
        private ExportStepConfiguration avroToCsvExportStepConf = new ExportStepConfiguration();
        private ImportExportS3StepConfiguration exportToS3 = new ImportExportS3StepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("segmentExportWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            initStepConf.setCustomerSpace(customerSpace);
            avroToCsvExportStepConf.setCustomerSpace(customerSpace);
            exportToS3.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder metadataSegmentExportId(String metadataSegmentExportId) {
            initStepConf.setMetadataSegmentExportId(metadataSegmentExportId);
            exportToS3.setAtlasExportId(metadataSegmentExportId);
            return this;
        }

        public Builder targetPath(String targetPath) {
            avroToCsvExportStepConf.setExportTargetPath(targetPath);
            return this;
        }

        public Builder podId(String podId) {
            avroToCsvExportStepConf.setPodId(podId);
            exportToS3.setPodId(podId);
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
            exportToS3.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            avroToCsvExportStepConf.setMicroServiceHostPort(microServiceHostPort);
            exportToS3.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public SegmentExportWorkflowConfiguration build() {
            avroToCsvExportStepConf.setExportFormat(ExportFormat.CSV);
            avroToCsvExportStepConf.setExportDestination(ExportDestination.FILE);
            avroToCsvExportStepConf.setUsingDisplayName(true);
            configuration.add(initStepConf);
            configuration.add(avroToCsvExportStepConf);
            configuration.add(exportToS3);
            return configuration;
        }

    }
}
