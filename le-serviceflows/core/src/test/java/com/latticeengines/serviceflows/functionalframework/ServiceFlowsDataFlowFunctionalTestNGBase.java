package com.latticeengines.serviceflows.functionalframework;

import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import com.latticeengines.dataflow.functionalframework.DataFlowCascadingTestNGBase;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-serviceflows-dataflow-context.xml" })
public abstract class ServiceFlowsDataFlowFunctionalTestNGBase extends DataFlowCascadingTestNGBase {
}
