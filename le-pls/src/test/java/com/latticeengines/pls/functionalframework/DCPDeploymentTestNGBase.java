package com.latticeengines.pls.functionalframework;

import javax.inject.Inject;

import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

public class DCPDeploymentTestNGBase extends PlsDeploymentTestNGBase {

    protected static final String TEST_TEMPLATE_DIR = "le-serviceapps/dcp/deployment/template";
    protected static final String TEST_TEMPLATE_NAME = "dcp-accounts-hard-coded.json";
    protected static final String TEST_TEMPLATE_VERSION = "1";
    protected static final String TEST_DATA_DIR = "le-serviceapps/dcp/deployment/testdata";

    protected String customerSpace;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    protected TestArtifactService testArtifactService;

    protected JobStatus waitForWorkflowStatus(String applicationId, boolean running) {
        return waitForWorkflowStatus(workflowProxy, applicationId, running);
    }
}

