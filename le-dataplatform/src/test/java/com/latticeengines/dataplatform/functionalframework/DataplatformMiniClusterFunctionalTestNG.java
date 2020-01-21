package com.latticeengines.dataplatform.functionalframework;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;

import com.latticeengines.yarn.functionalframework.YarnMiniClusterFunctionalTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dataplatform-context.xml" })
public class DataplatformMiniClusterFunctionalTestNG extends YarnMiniClusterFunctionalTestNGBase
        implements DataplatformFunctionalTestNGInterface {

    protected String suffix = this.getClass().getSimpleName() + "_" + generateUnique();

}
