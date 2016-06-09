package com.latticeengines.modelquality.functionalframework;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-modelquality-context.xml" })
public class ModelQualityFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

}
