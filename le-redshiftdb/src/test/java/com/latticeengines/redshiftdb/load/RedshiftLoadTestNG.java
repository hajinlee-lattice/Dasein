package com.latticeengines.redshiftdb.load;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(RedshiftLoadTestNG.class);

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
            log.error(ex.getMessage(), ex);
        }
    }

    @Test(groups = "load")
    public void testPerformance1() {
        int maxSleepTime = 1000;
        int stmtsPerTest = 10;
        int numSimultaneousUsers = 5;

        Set<LoadTestStatementType> stmtTypes = new HashSet<>();
        stmtTypes.add(LoadTestStatementType.Query_Multi_Conditional);

        RedshiftLoadTestHarness test = new RedshiftLoadTestHarness(numSimultaneousUsers, stmtsPerTest, stmtTypes,
                maxSleepTime, redshiftJdbcTemplate);

        String testSummary = "No of simultaneous users " + numSimultaneousUsers + "\n" + //
                "Statements per Thread: " + stmtsPerTest + "\n" + //
                "Sleep time interval: 0-" + maxSleepTime / 1000 + " sec";

        log.info("### Starting testPerformance1 ###");
        log.info(testSummary);
        log.info(test.run());
        log.info("### Finished testPerformance1 ###");
    }

    @Test(groups = "load")
    public void testPerformance2() {
        int maxSleepTime = 1000;
        int stmtsPerTest = 15;
        int numSimultaneousUsers = 10;

        Set<LoadTestStatementType> stmtTypes = new HashSet<>();
        stmtTypes.add(LoadTestStatementType.Query_Multi_Conditional);

        RedshiftLoadTestHarness test = new RedshiftLoadTestHarness(numSimultaneousUsers, stmtsPerTest, stmtTypes,
                maxSleepTime, redshiftJdbcTemplate);

        String testSummary = "No of simultaneous users " + numSimultaneousUsers + "\n" + //
                "Statements per Thread: " + stmtsPerTest + "\n" + //
                "Sleep time interval: 0-" + maxSleepTime / 1000 + " sec";

        log.info("### Starting testPerformance2 ###");
        log.info(testSummary);
        log.info(test.run());
        log.info("### Finished testPerformance2 ###");
    }

    @Test(groups = "load")
    public void testPerformance3() {
        int maxSleepTime = 1000;
        int stmtsPerTest = 15;
        int numSimultaneousUsers = 20;
        Set<LoadTestStatementType> stmtTypes = new HashSet<>();
        stmtTypes.add(LoadTestStatementType.Query_Multi_Conditional);

        RedshiftLoadTestHarness test = new RedshiftLoadTestHarness(numSimultaneousUsers, stmtsPerTest, stmtTypes,
                maxSleepTime, redshiftJdbcTemplate);

        String testSummary = "No of simultaneous users " + numSimultaneousUsers + "\n" + //
                "Statements per Thread: " + stmtsPerTest + "\n" + //
                "Sleep time interval: 0-" + maxSleepTime / 1000 + " sec";

        log.info("### Starting testPerformance3 ###");
        log.info(testSummary);
        log.info(test.run());
        log.info("### Finished testPerformance3 ###");
    }

    @Test(groups = "load")
    public void testPerformance4() {
        int maxSleepTime = 1000;
        int stmtsPerTest = 15;
        int numSimultaneousUsers = 25;
        Set<LoadTestStatementType> stmtTypes = new HashSet<>();
        stmtTypes.add(LoadTestStatementType.Query_Multi_Conditional);

        RedshiftLoadTestHarness test = new RedshiftLoadTestHarness(numSimultaneousUsers, stmtsPerTest, stmtTypes,
                maxSleepTime, redshiftJdbcTemplate);

        String testSummary = "No of simultaneous users " + numSimultaneousUsers + "\n" + //
                "Statements per Thread: " + stmtsPerTest + "\n" + //
                "Sleep time interval: 0-" + maxSleepTime / 1000 + " sec";

        log.info("### Starting testPerformance4 ###");
        log.info(testSummary);
        log.info(test.run());
        log.info("### Finished testPerformance4 ###");
    }
}
