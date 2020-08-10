package com.latticeengines.domain.exposed.serviceflows.dcp;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.DCPReportImportExportConfiguration;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.RollupDataReportStepConfiguration;

public class DCPDataReportWorkflowConfiguration extends BaseDCPWorkflowConfiguration {

    public static final String WORKFLOW_NAME = "dcpDataReportWorkflow";

    public DCPDataReportWorkflowConfiguration() {
    }

    public static class Builder {
        private DCPDataReportWorkflowConfiguration configuration = new DCPDataReportWorkflowConfiguration();

        private RollupDataReportStepConfiguration generateReport = new RollupDataReportStepConfiguration();

        private DCPReportImportExportConfiguration importExport = new DCPReportImportExportConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            generateReport.setCustomerSpace(customerSpace);
            importExport.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            generateReport.setInternalResourceHostPort(internalResourceHostPort);
            importExport.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            generateReport.setInternalResourceHostPort(microServiceHostPort);
            importExport.setInternalResourceHostPort(microServiceHostPort);
            return this;
        }

        public Builder rootId(String rootId) {
            generateReport.setRootId(rootId);
            importExport.setRootId(rootId);
            return this;
        }

        public Builder level(DataReportRecord.Level level) {
            generateReport.setLevel(level);
            importExport.setLevel(level);
            return this;
        }

        public Builder mode(DataReportMode mode) {
            generateReport.setMode(mode);
            importExport.setMode(mode);
            return this;
        }

        public DCPDataReportWorkflowConfiguration builder() {
            configuration.setContainerConfiguration(WORKFLOW_NAME, configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            configuration.add(generateReport);
            configuration.add(importExport);
            return configuration;
        }

    }
}
