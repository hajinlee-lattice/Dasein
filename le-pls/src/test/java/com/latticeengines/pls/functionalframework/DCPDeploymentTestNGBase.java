package com.latticeengines.pls.functionalframework;

import javax.inject.Inject;

import com.latticeengines.testframework.exposed.service.TestArtifactService;

public class DCPDeploymentTestNGBase extends PlsDeploymentTestNGBase {

    protected static final String TEST_TEMPLATE_DIR = "le-serviceapps/dcp/deployment/template";
    protected static final String TEST_TEMPLATE_NAME = "dcp-accounts-hard-coded.json";
    protected static final String TEST_TEMPLATE_VERSION = "1";
    protected String customerSpace;

    @Inject
    protected TestArtifactService testArtifactService;

}

