package com.latticeengines.propdata.match.testframework;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.domain.exposed.propdata.MatchClient;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-propdata-match-context.xml" })
public abstract class PropDataMatchFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @Value("${propdata.test.env}")
    protected String testEnv;

    @Value("${propdata.test.match.client}")
    protected String testMatchClientName;

    protected MatchClient getMatchClient() {
        return MatchClient.valueOf(testMatchClientName);
    }

}
