package com.latticeengines.domain.exposed.serviceflows.cdl;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.OperationExecuteConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.StartMaintenanceConfiguration;

public class CDLOperationWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public CDLOperationWorkflowConfiguration() {
    }

    public static class Builder {

        private CDLOperationWorkflowConfiguration configuration = new CDLOperationWorkflowConfiguration();

        private StartMaintenanceConfiguration startMaintenanceConfiguration = new StartMaintenanceConfiguration();
        private OperationExecuteConfiguration operationExecuteConfiguration = new OperationExecuteConfiguration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("cdlOperationWorkflow", customerSpace, "cdlOperationWorkflow");
            startMaintenanceConfiguration.setCustomerSpace(customerSpace);
            operationExecuteConfiguration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder internalResourceHostPort(String internalResourceHostPort) {
            startMaintenanceConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            operationExecuteConfiguration.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public Builder microServiceHostPort(String microServiceHostPort) {
            startMaintenanceConfiguration.setMicroServiceHostPort(microServiceHostPort);
            operationExecuteConfiguration.setMicroServiceHostPort(microServiceHostPort);
            return this;
        }

        public Builder maintenanceOperationConfiguration(
                MaintenanceOperationConfiguration maintenanceOperationConfiguration) {
            operationExecuteConfiguration.setMaintenanceOperationConfiguration(maintenanceOperationConfiguration);
            return this;
        }

        public CDLOperationWorkflowConfiguration build() {
            configuration.add(startMaintenanceConfiguration);
            configuration.add(operationExecuteConfiguration);
            return configuration;
        }
    }
}
