package com.latticeengines.redshiftdb.load;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.springframework.jdbc.core.JdbcTemplate;

public class RedshiftLoadTestHarness {

    private int loadLevel = 1;
    private boolean timeseries;
    private boolean encrypted;
    private boolean includeDDL;
    private JdbcTemplate redshiftJdbcTemplate;
    private Random rng;
    private int noOfTestsPerThread;

    public RedshiftLoadTestHarness(int load, JdbcTemplate jdbcTemplate, int noOfTests) {
        loadLevel = load;
        redshiftJdbcTemplate = jdbcTemplate;
        rng = new Random();
        noOfTestsPerThread = noOfTests;
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
                tester = new QueryLoadTest(i + 1, redshiftJdbcTemplate, noOfTestsPerThread);
            } else if (testType == LoadTestType.DDL) {
                tester = new AlterDDLLoadTest(i + 1, redshiftJdbcTemplate, noOfTestsPerThread);
            }
            testers.add(tester);
        }

        // Execute in parallel
        testers.parallelStream().forEach(tester -> tester.execute());

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
