package com.latticeengines.scoringapi.functionalframework;

import java.util.UUID;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.context.web.WebAppConfiguration;

@WebAppConfiguration
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-scoringapi-context.xml" })
public class ScoringApiFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    protected String generateRandomModelId() {
        return String.format("ms__%s-PLSModel", UUID.randomUUID());
    }

}
