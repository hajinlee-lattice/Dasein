package com.latticeengines.redshiftdb.load;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = { "classpath:test-redshiftdb-context.xml" })
public abstract class AbstractLoadTest implements Callable<String> {
    private static final Logger log = LoggerFactory.getLogger(RedshiftLoadTestNG.class);
    protected JdbcTemplate redshiftJdbcTemplate;
    protected String testerID;
    protected LoadTestStatementType sqlStatementType;
    protected List<Interval> runningTimeSpans;

    private final int numberOfStatements;
    private final int maxSleepTime;
    private Random rng = new Random();
    private List<ResultMetrics> metrics = new ArrayList<>();

    public AbstractLoadTest(int id, int noOfTestsPerThread, int sleepTime, JdbcTemplate jdbcTemplate) {
        testerID = "Tester" + id;
        redshiftJdbcTemplate = jdbcTemplate;
        runningTimeSpans = new ArrayList<>();
        numberOfStatements = noOfTestsPerThread;
        maxSleepTime = sleepTime;
    }

    public final void execute() {
        setup();

        for (int i = 0; i < numberOfStatements; i++) {
            try {
                String sql = getSqlStatement();
                DateTime start = DateTime.now();
                executeStatement(sql);
                DateTime end = DateTime.now();
                recordTime(start, end.getMillis() - start.getMillis());
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

    // All this method should do is to use the correct jdbc template method to
    // execute the sql passed.
    protected abstract void executeStatement(String sql);

    protected abstract String getSqlStatement();

    protected void setup() {
        // Let subclasses override if needed
    }

    protected void cleanup() {
        // Let subclasses override if needed
    }

    public void recordTime(DateTime startTime, long millis) {
        log.info(String.format("%s,%s,%sms", sqlStatementType,
                startTime.toString(DateTimeFormat.forPattern("hh:mm:ss.SSS")), millis));
        metrics.add(new ResultMetrics(startTime, millis));
    }

    protected String getSqlStatementFromType(LoadTestStatementType sqlStatementType) {
        try {
            return FileUtils.readFileToString(
                    new File(ClassLoader.getSystemResource("load/" + sqlStatementType.getScriptFileName()).getFile()));
        } catch (IOException ex) {
            return null;
        }
    }

    @Override
    public String call() {
        execute();
        return "";
    }

    public List<ResultMetrics> getMetrics() {
        return metrics;
    }
}
