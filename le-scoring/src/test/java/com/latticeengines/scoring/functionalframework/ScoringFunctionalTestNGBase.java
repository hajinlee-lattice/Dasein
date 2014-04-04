package com.latticeengines.scoring.functionalframework;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-scoring-context.xml" })
public class ScoringFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    protected RestTemplate restTemplate = new RestTemplate();
}
