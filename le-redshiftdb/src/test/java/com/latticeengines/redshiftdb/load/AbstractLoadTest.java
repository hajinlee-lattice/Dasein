package com.latticeengines.redshiftdb.load;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = { "classpath:test-redshiftdb-context.xml" })
public abstract class AbstractLoadTest {

    protected JdbcTemplate redshiftJdbcTemplate;
    protected String testerID;
    protected LoadTestStatementType sqlStatementType;
    protected List<Interval> runningTimeSpans;

    private final int numberOfStatements;
    private final int maxSleepTime = 1000;
    private Random rng = new Random();

    public AbstractLoadTest(int id, JdbcTemplate jdbcTemplate, int noOfTestsPerThread) {
        testerID = "Tester" + id;
        redshiftJdbcTemplate = jdbcTemplate;
        runningTimeSpans = new ArrayList<>();
        numberOfStatements = noOfTestsPerThread;
    }

    public final void execute() {
        setup();

        for (int i = 0; i < numberOfStatements; i++) {
            try {
                DateTime startTime = DateTime.now();
                executeStatement();
                DateTime endTime = DateTime.now();
                runningTimeSpans.add(new Interval(startTime, endTime));
                Thread.sleep(rng.nextInt(maxSleepTime));
            } catch (InterruptedException ex) {
                System.err.println("An InterruptedException was caught: " + ex.getMessage());
            } catch (Exception e) {
                // something went wrong, log and exit
                System.err
                        .println(testerID + " terminated due to and unexpected exception occurred: " + e.getMessage());
                break;
            }
        }

        cleanup();
    }

    public List<Interval> getRunningTimeSpans() {
        return runningTimeSpans;
    }

    protected abstract void executeStatement();

    protected void setup() {
        // Let subclasses override if needed
    }

    protected void cleanup() {
        // Let subclasses override if needed
    }

    @Override
    public String toString() {

        return getRunningTimeSpans().stream() //
                .map(span -> testerID + "(" + sqlStatementType.toString() + //
                        ") Duration: " + span.toDurationMillis() + "ms" + //
                        ", StartTime: " + span.getStart().toDateTimeISO() + "\n")
                .reduce((a, b) -> a + b).get();
    }

    protected String getSqlStatementFromType(LoadTestStatementType sqlStatementType) {
        try {
            return FileUtils.readFileToString(
                    new File(ClassLoader.getSystemResource("load/" + sqlStatementType.getScriptFileName()).getFile()));
        } catch (IOException ex) {
            return null;
        }
    }
}
