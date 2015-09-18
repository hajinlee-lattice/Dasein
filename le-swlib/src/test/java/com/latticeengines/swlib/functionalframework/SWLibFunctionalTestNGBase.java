package com.latticeengines.swlib.functionalframework;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-swlib-context.xml" })
public class SWLibFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

}
