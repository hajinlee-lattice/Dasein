package com.latticeengines.workflow.functionalframework;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

//@ContextConfiguration(classes = { DLOrchestrationWorkflow.class, StandaloneInfrastructureConfiguration.class })
@ContextConfiguration(locations = { "classpath:test-workflow-context.xml" })
public class WorkflowFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

}
