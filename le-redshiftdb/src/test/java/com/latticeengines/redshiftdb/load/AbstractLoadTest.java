package com.latticeengines.redshiftdb.load;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.Interval;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(locations = { "classpath:test-redshiftdb-context.xml" })
public abstract class AbstractLoadTest implements Callable<String> {
    private static final Log log = LogFactory.getLog(Tester.class);

    protected JdbcTemplate redshiftJdbcTemplate;
    protected String testerID;
    protected LoadTestStatementType sqlStatementType;
    protected List<Interval> runningTimeSpans;

    private final int numberOfStatements;
    private final int maxSleepTime;
    private Random rng = new Random();
    private Map<LoadTestStatementType, List<Long>> metrics = new HashMap<>();

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
                executeStatement();
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

    public void recordTime(LoadTestStatementType statement, long millis) {
        log.info(String.format("%s took %s ms", statement, millis));
        if (!metrics.containsKey(statement)) {
            metrics.put(statement, new ArrayList<>());
        }
        metrics.get(statement).add(millis);
    }

    protected String getSqlStatementFromType(LoadTestStatementType sqlStatementType) {
        try {
            return FileUtils.readFileToString(new File(ClassLoader.getSystemResource(
                    "load/" + sqlStatementType.getScriptFileName()).getFile()));
        } catch (IOException ex) {
            return null;
        }
    }

    @Override
    public String call() {
        execute();
        return "";
    }

    public Map<LoadTestStatementType, List<Long>> getMetrics() {
        return metrics;
    }
}
