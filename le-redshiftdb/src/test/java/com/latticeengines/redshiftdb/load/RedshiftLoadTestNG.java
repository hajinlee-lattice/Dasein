package com.latticeengines.redshiftdb.load;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-redshiftdb-context.xml" })
public class RedshiftLoadTestNG extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(RedshiftLoadTestNG.class);

    @Autowired
    @Qualifier(value = "redshiftJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Value("${redshift.test.load.tests.per.thread}")
    private int stmtsPerThread;

    @BeforeClass(groups = "load")
    public void setup() {
        // Drop columns that may have stayed alive after previous runs
        String querySql = "select \"column\"\n" + //
                "from pg_table_def\n" + //
                "where tablename = 'loadtesteventtable' and \"column\" like 'load_test_column_tester%'";
        try {
            List<String> badColumns = redshiftJdbcTemplate.queryForList(querySql, String.class);

            if (badColumns.size() > 0) {

                String dropColumnSql = FileUtils.readFileToString(new File(ClassLoader
                        .getSystemResource("load/" + LoadTestStatementType.DDL_Drop.getScriptFileName()).getFile()));

                for (String column : badColumns) {
                    redshiftJdbcTemplate.execute(dropColumnSql.replaceAll("load_test_column", column));
                }
            }
        } catch (IOException ex) {
            log.error(ex);
        }
    }

    @Test(groups = "load")
    public void testDDLWithSelects1() {
        int maxSleepTime = 1000;
        int stmtsPerTest = 15;
        int noOfTests = 5;

        RedshiftLoadTestHarness test = new RedshiftLoadTestHarness(noOfTests, stmtsPerTest, maxSleepTime,
                redshiftJdbcTemplate);

        String testSummary = "No of threads: " + noOfTests + "\n" + //
                "Statements per Thread: " + stmtsPerTest + "\n" + //
                "Sleep time interval: 0-" + maxSleepTime / 1000 + " sec";

        log.info("### Starting testDDLWithSelects1 ###");
        log.info(test.run());
        log.info(testSummary);
        log.info("### Finished testDDLWithSelects1 ###");
    }

    @Test(groups = "load")
    public void testDDLWithSelects2() {
        int maxSleepTime = 1000;
        int stmtsPerTest = 15;
        int noOfTests = 10;

        RedshiftLoadTestHarness test = new RedshiftLoadTestHarness(noOfTests, stmtsPerTest, maxSleepTime,
                redshiftJdbcTemplate);

        String testSummary = "No of threads: " + noOfTests + "\n" + //
                "Statements per Thread: " + stmtsPerTest + "\n" + //
                "Sleep time interval: 0-" + maxSleepTime / 1000 + " sec";

        log.info("### Starting testDDLWithSelects2 ###");
        log.info(test.run());
        log.info(testSummary);
        log.info("### Finished testDDLWithSelects2 ###");
    }

    @Test(groups = "load")
    public void testDDLWithSelects3() {
        int maxSleepTime = 1000;
        int stmtsPerTest = 15;
        int noOfTests = 15;

        RedshiftLoadTestHarness test = new RedshiftLoadTestHarness(noOfTests, stmtsPerTest, maxSleepTime,
                redshiftJdbcTemplate);

        String testSummary = "No of threads: " + noOfTests + "\n" + //
                "Statements per Thread: " + stmtsPerTest + "\n" + //
                "Sleep time interval: 0-" + maxSleepTime / 1000 + " sec";

        log.info("### Starting testDDLWithSelects3 ###");
        log.info(test.run());
        log.info(testSummary);
        log.info("### Finished testDDLWithSelects3 ###");
    }

    @Test(groups = "load")
    public void testDDLWithSelects4() {
        int maxSleepTime = 1000;
        int stmtsPerTest = 15;
        int noOfTests = 25;

        RedshiftLoadTestHarness test = new RedshiftLoadTestHarness(noOfTests, stmtsPerTest, maxSleepTime,
                redshiftJdbcTemplate);

        String testSummary = "No of threads: " + noOfTests + "\n" + //
                "Statements per Thread: " + stmtsPerTest + "\n" + //
                "Sleep time interval: 0-" + maxSleepTime / 1000 + " sec";

        log.info("### Starting testDDLWithSelects4 ###");
        log.info(test.run());
        log.info(testSummary);
        log.info("### Finished testDDLWithSelects4 ###");
    }

    @Test(groups = "load")
    public void testDDLWithSelects5() {
        int maxSleepTime = 1000;
        int stmtsPerTest = 15;
        int noOfTests = 50;

        RedshiftLoadTestHarness test = new RedshiftLoadTestHarness(noOfTests, stmtsPerTest, maxSleepTime,
                redshiftJdbcTemplate);

        String testSummary = "No of threads: " + noOfTests + "\n" + //
                "Statements per Thread: " + stmtsPerTest + "\n" + //
                "Sleep time interval: 0-" + maxSleepTime / 1000 + " sec";

        log.info("### Starting testDDLWithSelects5 ###");
        log.info(test.run());
        log.info(testSummary);
        log.info("### Finished testDDLWithSelects5 ###");
    }
}
