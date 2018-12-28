package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ComputeOrphanRecordsStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportOrphansToS3StepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;

public class OrphanRecordsExportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String WORKFLOW_NAME = "orphanRecordsExportWorkflow";
    public static final String CREATED_BY = "CREATED_BY";
    public static final String EXPORT_ID = "EXPORT_ID";
    public static final String ARTIFACT_TYPE = "ARTIFACT_TYPE";

    public static class Builder {
        private OrphanRecordsExportWorkflowConfiguration configuration = new OrphanRecordsExportWorkflowConfiguration();
        private ComputeOrphanRecordsStepConfiguration computeOrphans = new ComputeOrphanRecordsStepConfiguration();
        private ExportStepConfiguration avroToCsvExport = new ExportStepConfiguration();
        private ExportOrphansToS3StepConfiguration exportToS3 = new ExportOrphansToS3StepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration(WORKFLOW_NAME, customerSpace,
                    configuration.getClass().getSimpleName());
            computeOrphans.setCustomerSpace(customerSpace);
            avroToCsvExport.setCustomerSpace(customerSpace);
            exportToS3.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder orphanRecordExportId(String orphanRecordExportId) {
            computeOrphans.setOrphanRecordsExportId(orphanRecordExportId);
            exportToS3.setExportId(orphanRecordExportId);
            return this;
        }

        public Builder orphanRecordsType(OrphanRecordsType type) {
            computeOrphans.setOrphanRecordsType(type);
            exportToS3.setOrphanRecordsType(type);
            return this;
        }

        public Builder targetPath(String targetPath) {
            computeOrphans.setTargetPath(targetPath);
            avroToCsvExport.setExportTargetPath(targetPath);
            exportToS3.setSourcePath(targetPath);
            return this;
        }

        public Builder podId(String podId) {
            avroToCsvExport.setPodId(podId);
            exportToS3.setPodId(podId);
            return this;
        }

        public Builder exportInputPath(String exportInputPath) {
            avroToCsvExport.setExportInputPath(exportInputPath);
            return this;
        }

        public Builder targetTableName(String tableName) {
            computeOrphans.setTargetTableName(tableName);
            avroToCsvExport.setTableName(tableName);
            return this;
        }

        public Builder workflow(String workflowName) {
            configuration.setWorkflowName(workflowName);
            configuration.setName(workflowName);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            avroToCsvExport.setProperties(inputProperties);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            avroToCsvExport.setInternalResourceHostPort(internalResourceHostPort);
            exportToS3.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            avroToCsvExport.setMicroServiceHostPort(microServiceHostPort);
            exportToS3.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder transactionTableName(String tableName){
            computeOrphans.setTransactionTableName(tableName);
            return this;
        }

        public Builder accountTableName(String tableName){
            computeOrphans.setAccountTableName(tableName);
            return this;
        }

        public Builder contactTableName(String tableName) {
            computeOrphans.setContactTableName(tableName);
            return this;
        }

        public Builder productTableName(String tableName){
            computeOrphans.setProductTableName(tableName);
            return this;
        }

        public Builder exportMergeFile(Boolean exportMergeFile){
            avroToCsvExport.setExportMergedFile(exportMergeFile);
            return this;
        }

        public Builder mergedFileName(String filename){
            avroToCsvExport.setMergedFileName(filename);
            return this;
        }

        public Builder dataCollectionName(String dataCollectionName) {
            computeOrphans.setDataCollectionName(dataCollectionName);
            exportToS3.setDataCollectionName(dataCollectionName);
            return this;
        }

        public Builder dataCollectionVersion(DataCollection.Version version) {
            computeOrphans.setDataCollectionVersion(version);
            exportToS3.setVersion(version);
            return this;
        }

        public OrphanRecordsExportWorkflowConfiguration build() {
            avroToCsvExport.setExportFormat(ExportFormat.CSV);
            avroToCsvExport.setExportDestination(ExportDestination.FILE);
            avroToCsvExport.setUsingDisplayName(true);

            configuration.add(computeOrphans);
            configuration.add(avroToCsvExport);
            configuration.add(exportToS3);
            return configuration;
        }
    }
}
