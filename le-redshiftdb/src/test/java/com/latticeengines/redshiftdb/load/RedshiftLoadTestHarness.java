package com.latticeengines.redshiftdb.load;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;

public class RedshiftLoadTestHarness {

    private int loadLevel = 1;
    private boolean timeseries;
    private boolean encrypted;
    private boolean includeDDL;
    private JdbcTemplate redshiftJdbcTemplate;
    private Random rng;
    private int noOfTestsPerThread;
    private int maxSleepTime;
    private static final Log log = LogFactory.getLog(RedshiftLoadTestNG.class);

    public RedshiftLoadTestHarness(int load, int noOfTests, int sleepTime, JdbcTemplate jdbcTemplate) {
        loadLevel = load;
        redshiftJdbcTemplate = jdbcTemplate;
        rng = new Random();
        noOfTestsPerThread = noOfTests;
        maxSleepTime = sleepTime;
    }

    public String run() {
        String resultString = "";
        if (loadLevel < 1) {
            return "loadLevel needs to be greater than 1. Terminating Test";
        }

        // setup testers as per the config
        List<AbstractLoadTest> testers = new ArrayList<>();
        for (int i = 0; i < getLoadLevel(); i++) {
            AbstractLoadTest tester = null;
            int randomTestNum = rng.nextInt(LoadTestType.values().length);
            LoadTestType testType = LoadTestType.values()[randomTestNum];

            if (testType == LoadTestType.Query) {
                tester = new QueryLoadTest(i + 1, noOfTestsPerThread, maxSleepTime, redshiftJdbcTemplate);
            } else if (testType == LoadTestType.DDL) {
                tester = new AlterDDLLoadTest(i + 1, noOfTestsPerThread, maxSleepTime, redshiftJdbcTemplate);
            }
            testers.add(tester);
        }

        // Execute in parallel
        try {
            ExecutorService executor = Executors.newCachedThreadPool();
            List<Future<String>> results = executor.invokeAll(testers);
            for (Future<String> result : results) {
                result.get();
            }
            executor.shutdown();
        } catch (InterruptedException e) {
            log.error(e);
        } catch (ExecutionException e) {
            log.error(e);
        }

        resultString = "\nTest,StatementType,Duration,StartTime\n";
        for (AbstractLoadTest tester : testers) {
            resultString += tester.toString() + "\n";
        }

        return resultString;
    }

    public int getLoadLevel() {
        return loadLevel;
    }

    public void setLoadLevel(int loadLevel) {
        this.loadLevel = loadLevel;
    }

    public boolean isTimeseries() {
        return timeseries;
    }

    public void setTimeseries(boolean timeseries) {
        this.timeseries = timeseries;
    }

    public boolean isEncrypted() {
        return encrypted;
    }

    public void setEncrypted(boolean encrypted) {
        this.encrypted = encrypted;
    }

    public boolean isIncludeDDL() {
        return includeDDL;
    }

    public void setIncludeDDL(boolean includeDDL) {
        this.includeDDL = includeDDL;
    }
}
