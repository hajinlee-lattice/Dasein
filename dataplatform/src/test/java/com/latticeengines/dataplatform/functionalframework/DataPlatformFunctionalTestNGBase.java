package com.latticeengines.dataplatform.functionalframework;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;


@TestExecutionListeners({
    DirtiesContextTestExecutionListener.class
})
@ContextConfiguration(locations = {
    "classpath:test-dataplatform-context.xml"
})
public class DataPlatformFunctionalTestNGBase extends AbstractTestNGSpringContextTests {
}

