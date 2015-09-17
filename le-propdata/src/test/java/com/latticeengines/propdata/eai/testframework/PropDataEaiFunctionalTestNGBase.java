package com.latticeengines.propdata.eai.testframework;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-propdata-eai-context.xml" })
public abstract class PropDataEaiFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

}
