package com.latticeengines.dataplatform.functionalframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;

import com.latticeengines.yarn.functionalframework.YarnMiniClusterFunctionalTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dataplatform-context.xml" })
public class DataplatformMiniClusterFunctionalTestNG extends YarnMiniClusterFunctionalTestNGBase
        implements DataplatformFunctionalTestNGInterface {

    protected static final Log log = LogFactory.getLog(DataplatformMiniClusterFunctionalTestNG.class);

    protected String suffix = this.getClass().getSimpleName() + "_" + generateUnique();

    @Value("${dataplatform.hdfs.stack:}")
    protected String stackName;

    @Value("${dataplatform.queue.scheme:legacy}")
    protected String queueScheme;

}
