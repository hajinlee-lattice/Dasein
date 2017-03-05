package com.latticeengines.datacloud.dataflow.framework;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;

import com.latticeengines.dataflow.functionalframework.DataFlowCascadingTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-datacloud-dataflow-context.xml" })
public abstract class DataCloudDataFlowFunctionalTestNGBase extends DataFlowCascadingTestNGBase {
}
