package com.latticeengines.cdl.workflow;

import org.springframework.test.context.ContextConfiguration;

import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml", "classpath:test-serviceflows-cdl-context.xml" })
public abstract class CDLWorkflowFunctionalTestNGBase extends ServiceFlowsDataFlowFunctionalTestNGBase {

}
