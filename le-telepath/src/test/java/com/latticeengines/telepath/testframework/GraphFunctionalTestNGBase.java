package com.latticeengines.telepath.testframework;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@ContextConfiguration(locations = { "classpath:test-telepath-context.xml" })
public class GraphFunctionalTestNGBase extends AbstractTestNGSpringContextTests {
}
