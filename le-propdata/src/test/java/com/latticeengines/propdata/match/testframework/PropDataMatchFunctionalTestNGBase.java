package com.latticeengines.propdata.match.testframework;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.domain.exposed.propdata.MatchClient;
import com.latticeengines.monitor.exposed.metric.service.MetricService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-propdata-match-context.xml" })
public abstract class PropDataMatchFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @Value("${propdata.test.env}")
    protected String testEnv;

    @Value("${propdata.test.match.client}")
    protected String testMatchClientName;

    @Autowired
    private MetricService metricService;

    @PostConstruct
    private void postConstruct() {
        metricService.disable();
    }

    protected MatchClient getMatchClient() {
        return MatchClient.valueOf(testMatchClientName);
    }

}
