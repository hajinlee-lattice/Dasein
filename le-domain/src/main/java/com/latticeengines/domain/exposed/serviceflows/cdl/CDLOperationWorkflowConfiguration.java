package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.CleanupByUploadWrapperConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.DeleteFileUploadStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.OperationExecuteConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.StartMaintenanceConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class CDLOperationWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public CDLOperationWorkflowConfiguration() {
    }

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.DataCloud.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {

        private CDLOperationWorkflowConfiguration configuration = new CDLOperationWorkflowConfiguration();

        private DeleteFileUploadStepConfiguration deleteFileUploadStepConfiguration = new DeleteFileUploadStepConfiguration();
        private StartMaintenanceConfiguration startMaintenanceConfiguration = new StartMaintenanceConfiguration();
        private OperationExecuteConfiguration operationExecuteConfiguration = new OperationExecuteConfiguration();
        private CleanupByUploadWrapperConfiguration cleanupByUploadWrapperConfiguration = new CleanupByUploadWrapperConfiguration();
        private ImportExportS3StepConfiguration importExportS3 = new ImportExportS3StepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("cdlOperationWorkflow", customerSpace,
                    configuration.getClass().getSimpleName());
            deleteFileUploadStepConfiguration.setCustomerSpace(customerSpace);
            startMaintenanceConfiguration.setCustomerSpace(customerSpace);
            operationExecuteConfiguration.setCustomerSpace(customerSpace);
            cleanupByUploadWrapperConfiguration.setCustomerSpace(customerSpace);
            importExportS3.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            deleteFileUploadStepConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            startMaintenanceConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            operationExecuteConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            cleanupByUploadWrapperConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            importExportS3.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            deleteFileUploadStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
            startMaintenanceConfiguration.setMicroServiceHostPort(microServiceHostPort);
            operationExecuteConfiguration.setMicroServiceHostPort(microServiceHostPort);
            importExportS3.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder maintenanceOperationConfiguration(
                MaintenanceOperationConfiguration maintenanceOperationConfiguration) {
            operationExecuteConfiguration.setMaintenanceOperationConfiguration(maintenanceOperationConfiguration);
            cleanupByUploadWrapperConfiguration.setMaintenanceOperationConfiguration(maintenanceOperationConfiguration);
            configuration.setUserId(maintenanceOperationConfiguration.getOperationInitiator());
            return this;
        }

        public Builder tableName(String tableName) {
            deleteFileUploadStepConfiguration.setTableName(tableName);
            cleanupByUploadWrapperConfiguration.setTableName(tableName);
            return this;
        }

        public Builder filePath(String filePath) {
            deleteFileUploadStepConfiguration.setFilePath(filePath);
            return this;
        }

        public Builder isCleanupByUpload(boolean cleanupByUpload, boolean useDLData) {
            deleteFileUploadStepConfiguration.setSkipStep(!cleanupByUpload || useDLData);
            cleanupByUploadWrapperConfiguration.setSkipStep(!cleanupByUpload);
            operationExecuteConfiguration.setSkipStep(cleanupByUpload);
            return this;
        }

        public Builder businessEntity(BusinessEntity businessEntity) {
            startMaintenanceConfiguration.setEntity(businessEntity);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public CDLOperationWorkflowConfiguration build() {
            configuration.add(deleteFileUploadStepConfiguration);
            configuration.add(startMaintenanceConfiguration);
            configuration.add(operationExecuteConfiguration);
            configuration.add(cleanupByUploadWrapperConfiguration);
            configuration.add(importExportS3);
            return configuration;
        }
    }
}
