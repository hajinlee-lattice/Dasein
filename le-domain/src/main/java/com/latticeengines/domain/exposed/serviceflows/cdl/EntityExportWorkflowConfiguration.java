package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.EntityExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class EntityExportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {

        EntityExportWorkflowConfiguration configuration = new EntityExportWorkflowConfiguration();
        ImportExportS3StepConfiguration importS3 = new ImportExportS3StepConfiguration();
        EntityExportStepConfiguration step = new EntityExportStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            importS3.setCustomerSpace(customerSpace);
            step.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder dataCollectionVersion(DataCollection.Version version) {
            importS3.setVersion(version);
            step.setDataCollectionVersion(version);
            return this;
        }

        public Builder saveToDropfolder(boolean save2Dropfolder) {
            step.setSaveToDropfolder(save2Dropfolder);
            return this;
        }

        public Builder compressResult(boolean compress) {
            step.setCompressResult(compress);
            return this;
        }

        public Builder atlasExportId(String atlasExportId) {
            importS3.setAtlasExportId(atlasExportId);
            step.setAtlasExportId(atlasExportId);
            return this;
        }

        public EntityExportWorkflowConfiguration build() {
            configuration.setContainerConfiguration("entityExportWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            step.setAddExportTimestamp(true); // always add export timestamp
            configuration.add(importS3);
            configuration.add(step);
            return configuration;
        }

    }

}
