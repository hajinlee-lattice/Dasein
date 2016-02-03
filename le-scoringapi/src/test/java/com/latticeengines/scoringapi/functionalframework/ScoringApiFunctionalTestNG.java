package com.latticeengines.scoringapi.functionalframework;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@ContextConfiguration(locations = { "classpath:test-scoringapi-context.xml" })
public class ScoringApiFunctionalTestNG extends AbstractTestNGSpringContextTests {

    public void beforeClass() {
    }

}
