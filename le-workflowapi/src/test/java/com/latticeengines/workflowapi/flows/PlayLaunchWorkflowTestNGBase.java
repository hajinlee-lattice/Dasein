package com.latticeengines.workflowapi.flows;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class PlayLaunchWorkflowTestNGBase extends WorkflowApiFunctionalTestNGBase {

    protected static final CustomerSpace PMML_CUSTOMERSPACE = CustomerSpace.parse("PmmlContract.PmmlTenant.Production");
    protected Tenant pmmlTenant;

    protected void setupForPlayLaunch() throws Exception {
        pmmlTenant = setupTenant(PMML_CUSTOMERSPACE);
        setupSecurityContext(pmmlTenant);
        setupUsers(PMML_CUSTOMERSPACE);
        setupCamille(PMML_CUSTOMERSPACE);
        setupHdfs(PMML_CUSTOMERSPACE);
    }

    protected void cleanUpAfterPlayLaunch() throws Exception {
        deleteTenantByRestCall(PMML_CUSTOMERSPACE.toString());
        cleanCamille(PMML_CUSTOMERSPACE);
        cleanHdfs(PMML_CUSTOMERSPACE);
    }

    protected PlayLaunchWorkflowConfiguration generatePlayLaunchWorkflowConfiguration() {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "playLaunchWorkflow");

        PlayLaunchWorkflowConfiguration workflowConfig = new PlayLaunchWorkflowConfiguration.Builder() //
                .customer(PMML_CUSTOMERSPACE) //
                .workflow("playLaunchWorkflow") //
                .build();

        return workflowConfig;
    }
}
