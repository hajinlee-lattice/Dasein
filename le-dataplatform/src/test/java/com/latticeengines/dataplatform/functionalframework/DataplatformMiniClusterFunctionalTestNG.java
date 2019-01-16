package com.latticeengines.dataplatform.functionalframework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;

import com.latticeengines.yarn.functionalframework.YarnMiniClusterFunctionalTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dataplatform-context.xml" })
public class DataplatformMiniClusterFunctionalTestNG extends YarnMiniClusterFunctionalTestNGBase
        implements DataplatformFunctionalTestNGInterface {

    protected static final Logger log = LoggerFactory.getLogger(DataplatformMiniClusterFunctionalTestNG.class);

    protected String suffix = this.getClass().getSimpleName() + "_" + generateUnique();

}
