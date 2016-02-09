package com.latticeengines.leadprioritization.workflow;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.importdata.ImportStepConfiguration;
import com.latticeengines.serviceflows.workflow.report.BaseReportStepConfiguration;

public class ImportEventTableWorkflowConfiguration extends WorkflowConfiguration {
    public static class Builder {
        private ImportEventTableWorkflowConfiguration configuration = new ImportEventTableWorkflowConfiguration();
        private ImportStepConfiguration importData = new ImportStepConfiguration();
        private BaseReportStepConfiguration registerReport = new BaseReportStepConfiguration();

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

        public Builder sourceType(SourceType sourceType) {
            importData.setSourceType(sourceType);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            importData.setInternalResourceHostPort(internalResourceHostPort);
            registerReport.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder reportName(String reportName) {
            registerReport.setReportName(reportName);
            return this;
        }

        public ImportEventTableWorkflowConfiguration build() {
            configuration.add(importData);
            configuration.add(registerReport);
            return configuration;
        }
    }
}
