package com.latticeengines.leadprioritization.workflow;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;
import com.latticeengines.serviceflows.workflow.importdata.ImportStepConfiguration;

public class ImportEventTableConfiguration extends WorkflowConfiguration {
    public static class Builder {
        private ImportEventTableConfiguration configuration = new ImportEventTableConfiguration();
        private ImportStepConfiguration importData = new ImportStepConfiguration();
        private MicroserviceStepConfiguration registerReport = new MicroserviceStepConfiguration();

        public Builder microServiceHostPort(String microServiceHostPort) {
            importData.setMicroServiceHostPort(microServiceHostPort);
            registerReport.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder customer(CustomerSpace customerSpace) {
            importData.setCustomerSpace(customerSpace);
            registerReport.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder sourceFileName(String sourceFileName) {
            importData.setSourceFileName(sourceFileName);
            return this;
        }

        public Builder sourceFileType(SourceType sourceType) {
            importData.setSourceType(sourceType);
            return this;
        }

        public ImportEventTableConfiguration build() {
            configuration.add(importData);
            configuration.add(registerReport);
            return configuration;
        }
    }
}
