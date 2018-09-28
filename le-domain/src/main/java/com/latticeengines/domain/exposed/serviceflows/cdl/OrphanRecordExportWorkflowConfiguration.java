package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.OrphanRecordExportConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToS3StepConfiguration;

import java.util.Map;

public class OrphanRecordExportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String EXPORT_OUTPUT_PATH = "EXPORT_OUTPUT_PATH";
    public static final String WORK_FLOW_NAME = "OrphanRecordExportWorkflow";

    public static class Builder{
        private OrphanRecordExportWorkflowConfiguration configuration = new OrphanRecordExportWorkflowConfiguration();
        private OrphanRecordExportConfiguration stepConf = new OrphanRecordExportConfiguration();
        private ExportStepConfiguration avroToCsvExportStepConf = new ExportStepConfiguration();
        private ExportToS3StepConfiguration exportToS3 = new ExportToS3StepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration(WORK_FLOW_NAME, customerSpace,
                    configuration.getClass().getSimpleName());
            stepConf.setCustomerSpace(customerSpace);
            avroToCsvExportStepConf.setCustomerSpace(customerSpace);
            exportToS3.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder orphanRecordExportId(String orphanRecordExportId) {
            stepConf.setOrphanRecordExportId(orphanRecordExportId);
            exportToS3.setMetadataSegmentExportId(orphanRecordExportId);
            return this;
        }


        public Builder targetPath(String targetPath) {
            avroToCsvExportStepConf.setExportTargetPath(targetPath);
            stepConf.setTargetPath(targetPath);
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
            stepConf.setTargetTableName(tableName);
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

        public Builder txnTableName(String tableName){
            stepConf.setTxnTableName(tableName);
            return this;
        }

        public Builder accountTableName(String tableName){
            stepConf.setAccountTableName(tableName);
            return this;
        }

        public Builder productTableName(String tableName){
            stepConf.setProductTableName(tableName);
            return this;
        }

        public Builder exportMergeFile(Boolean exportMergeFile){
            avroToCsvExportStepConf.setExportMergedFile(exportMergeFile);
            return this;
        }

        public Builder mergedFileName(String filename){
            avroToCsvExportStepConf.setMergedFileName(filename);
            return this;
        }

        public OrphanRecordExportWorkflowConfiguration build() {
            avroToCsvExportStepConf.setExportFormat(ExportFormat.CSV);
            avroToCsvExportStepConf.setExportDestination(ExportDestination.FILE);
            avroToCsvExportStepConf.setUsingDisplayName(true);

            configuration.add(stepConf);
            configuration.add(avroToCsvExportStepConf);
            configuration.add(exportToS3);
            return configuration;
        }
    }
}
