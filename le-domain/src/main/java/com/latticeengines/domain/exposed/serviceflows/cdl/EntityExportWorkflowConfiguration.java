package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.ArrayList;
import java.util.Collection;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.EntityExportStepConfiguration;

public class EntityExportWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static class Builder {

        EntityExportWorkflowConfiguration configuration = new EntityExportWorkflowConfiguration();
        EntityExportStepConfiguration step = new EntityExportStepConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            step.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder dataCollectionVersion(DataCollection.Version version) {
            step.setDataCollectionVersion(version);
            return this;
        }

        public Builder saveToDropfolder(boolean save2Dropfolder) {
            step.setSaveToDropfolder(save2Dropfolder);
            return this;
        }

        public Builder frontEndQuery(FrontEndQuery frontEndQuery) {
            step.setFrontEndQuery(frontEndQuery);
            return this;
        }

        public Builder exportEntities(Collection<ExportEntity> entities) {
            step.setExportEntities(new ArrayList<>(entities));
            return this;
        }

        public Builder compressResult(boolean compress) {
            step.setCompressResult(compress);
            return this;
        }

        public EntityExportWorkflowConfiguration build() {
            configuration.setContainerConfiguration("entityExportWorkflow", configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(step);
            return configuration;
        }

    }

}
