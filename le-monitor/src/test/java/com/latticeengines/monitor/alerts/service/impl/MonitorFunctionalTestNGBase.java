package com.latticeengines.monitor.alerts.service.impl;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:monitor-alerts-context.xml",
        "classpath:monitor-alerts-properties-context.xml" })
public class MonitorFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

}
