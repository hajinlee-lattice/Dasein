package com.latticeengines.pls.functionalframework;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

public class DCPDeploymentTestNGBase extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DCPDeploymentTestNGBase.class);

    protected static final String TEST_TEMPLATE_DIR = "le-serviceapps/dcp/deployment/template";
    protected static final String TEST_TEMPLATE_VERSION = "3";
    protected static final String TEST_TEMPLATE_NAME = "dcp-accounts-hard-coded.json";

    protected static final String TEST_DATA_DIR = "le-serviceapps/dcp/deployment/testdata";
    protected static final String TEST_DATA_VERSION = "5";
    protected static final String TEST_ACCOUNT_DATA_FILE = "Account_1_900.csv";

    protected String customerSpace;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    protected TestArtifactService testArtifactService;

    protected JobStatus waitForWorkflowStatus(String applicationId, boolean running) {
        log.info("Running workflow as " + applicationId);
        return waitForWorkflowStatus(workflowProxy, applicationId, running);
    }
}

