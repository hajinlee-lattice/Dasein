package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateIntentAlertArtifactsStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.SendIntentAlertEmailStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportTableRoleFromS3StepConfiguration;

public class GenerateIntentEmailAlertWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String NAME = "GenerateIntentEmailAlertWorkflowConfiguration";
    public static final String WORKFLOW_NAME = "generateIntentEmailAlertWorkflow";

    public static class Builder {
        private GenerateIntentEmailAlertWorkflowConfiguration configuration = new GenerateIntentEmailAlertWorkflowConfiguration();
        private ImportTableRoleFromS3StepConfiguration importTableRoleFromS3StepConfiguration =
                new ImportTableRoleFromS3StepConfiguration();
        private GenerateIntentAlertArtifactsStepConfiguration generateIntentArtifacts = new GenerateIntentAlertArtifactsStepConfiguration();
        private SendIntentAlertEmailStepConfiguration sendIntentAlertEmail = new SendIntentAlertEmailStepConfiguration();

        public GenerateIntentEmailAlertWorkflowConfiguration.Builder customer(CustomerSpace customerSpace) {
            configuration.setCustomerSpace(customerSpace);
            importTableRoleFromS3StepConfiguration.setCustomerSpace(customerSpace);
            generateIntentArtifacts.setCustomerSpace(customerSpace);
            sendIntentAlertEmail.setCustomerSpace(customerSpace);
            return this;
        }

        public GenerateIntentEmailAlertWorkflowConfiguration.Builder workflow(String workflowName) {
            configuration.setWorkflowName(workflowName);
            configuration.setName(workflowName);
            return this;
        }

        public GenerateIntentEmailAlertWorkflowConfiguration.Builder internalResourceHostPort(
                String internalResourceHostPort) {
            configuration.setInternalResourceHostPort(internalResourceHostPort);
            generateIntentArtifacts.setInternalResourceHostPort(internalResourceHostPort);
            sendIntentAlertEmail.setInternalResourceHostPort(internalResourceHostPort);
            return this;
        }

        public GenerateIntentEmailAlertWorkflowConfiguration build() {
            configuration.setContainerConfiguration(WORKFLOW_NAME, configuration.getCustomerSpace(),
                    configuration.getClass().getSimpleName());
            importTableRoleFromS3StepConfiguration.setTableRoleInCollections(getTableRole());
            configuration.add(importTableRoleFromS3StepConfiguration);
            configuration.add(generateIntentArtifacts);
            configuration.add(sendIntentAlertEmail);
            return configuration;
        }

        private List<TableRoleInCollection> getTableRole() {
            List<TableRoleInCollection> list = new ArrayList<>();
            list.add(TableRoleInCollection.ConsolidatedActivityStream);
            list.add(TableRoleInCollection.MetricsGroup);
            list.add(TableRoleInCollection.LatticeAccount);
            return list;
        }

    }
}
