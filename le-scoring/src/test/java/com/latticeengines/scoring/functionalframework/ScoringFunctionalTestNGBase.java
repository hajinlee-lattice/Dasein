package com.latticeengines.scoring.functionalframework;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;

import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-scoring-context.xml" })
public class ScoringFunctionalTestNGBase extends YarnFunctionalTestNGBase {

    @Value("${dataplatform.customer.basedir}")
    protected String customerBaseDir;

}
