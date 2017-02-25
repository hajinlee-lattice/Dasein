package com.latticeengines.redshiftdb.load;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-redshiftdb-context.xml" })
public class RedshiftLoadTestNG  extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(RedshiftLoadTestNG.class);

    private static final String TABLE_NAME = "EventTable";

    @Autowired
    @Qualifier(value = "redshiftJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Value("${redshift.test.load.tests.per.thread}")
    private int noOfTests;

    @Test(groups = "load")
    public void testDDLWithSelects() {
        RedshiftLoadTestHarness test = new RedshiftLoadTestHarness(25, redshiftJdbcTemplate, noOfTests);

        log.info(test.run());
    }
}
