package com.latticeengines.redshiftdb.load;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private JdbcTemplate redshiftJdbcTemplate;
    private Random rng;
    private List<LoadTestStatementType> statements;
    private int maxSleepTime;
    private static final Log log = LogFactory.getLog(RedshiftLoadTestNG.class);
    List<AbstractLoadTest> testers = new ArrayList<>();

    public RedshiftLoadTestHarness(int load, List<LoadTestStatementType> stmts, int sleepTime, JdbcTemplate jdbcTemplate) {
        loadLevel = load;
        redshiftJdbcTemplate = jdbcTemplate;
        rng = new Random();
        statements = stmts;
        maxSleepTime = sleepTime;

        // setup testers as per the config
        for (int i = 0; i < getLoadLevel(); i++) {
            AbstractLoadTest tester = null;
            tester = new Tester(i + 1, statements, maxSleepTime, redshiftJdbcTemplate);
            testers.add(tester);
        }
    }

    public String run() {
        String resultString = "";
        if (loadLevel < 1) {
            return "loadLevel needs to be greater than 1. Terminating Test";
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

        return getMetrics().toString();
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

    public Map<LoadTestStatementType, Double> getMetrics() {
        Map<LoadTestStatementType, List<Long>> all = new HashMap<>();
        for (AbstractLoadTest tester : testers) {
            all.putAll(tester.getMetrics());
        }

        Map<LoadTestStatementType, Double> results = new HashMap<>();
        for (LoadTestStatementType statementType : all.keySet()) {
            double average = all.get(statementType).stream().mapToLong(x -> x).average().getAsDouble();
            results.put(statementType, average);
        }
        return results;
    }
}
