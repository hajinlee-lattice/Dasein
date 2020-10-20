package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;

import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml", "classpath:test-serviceflows-cdl-context.xml" })
public abstract class CDLWorkflowFunctionalTestNGBase extends ServiceFlowsDataFlowFunctionalTestNGBase {

    protected static final String TEST_AVRO_DIR = "le-serviceflows/cdl/functional/avro";
    protected static final String TEST_AVRO_VERSION = "1";


    @Inject
    protected TestArtifactService testArtifactService;
}
