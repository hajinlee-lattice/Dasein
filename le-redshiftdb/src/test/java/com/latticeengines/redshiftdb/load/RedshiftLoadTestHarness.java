package com.latticeengines.redshiftdb.load;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

public class RedshiftLoadTestHarness {

    private int loadLevel = 1;
    private JdbcTemplate redshiftJdbcTemplate;
    private static final Logger log = LoggerFactory.getLogger(RedshiftLoadTestNG.class);
    List<AbstractLoadTest> testers = new ArrayList<>();

    public RedshiftLoadTestHarness(int load, int stmtsPerTester, Set<LoadTestStatementType> possibleStmtTypes,
            int maxSleepTime, JdbcTemplate jdbcTemplate) {
        redshiftJdbcTemplate = jdbcTemplate;

        // setup testers as per the config
        Random random = new Random();
        if (possibleStmtTypes.size() < 1)
            possibleStmtTypes = new HashSet<>(Arrays.asList(LoadTestStatementType.Query_Numeric_Join));

        LoadTestStatementType[] typesArray = possibleStmtTypes
                .toArray(new LoadTestStatementType[possibleStmtTypes.size()]);

        for (int i = 0; i < load; ++i) {
            LoadTestStatementType statementType = typesArray[random.nextInt(possibleStmtTypes.size())];
            switch (statementType.getTesterType()) {
            case "QueryLoadTest":
                testers.add(new QueryLoadTest(i, stmtsPerTester, maxSleepTime, redshiftJdbcTemplate, statementType));
                break;

            case "AlterDDLLoadTest":
                testers.add(new AlterDDLLoadTest(i, stmtsPerTester, maxSleepTime, redshiftJdbcTemplate));
                break;

            case "QueryMultiConditionalTest":
                testers.add(new MultiConditionalQueryLoadTest(i, stmtsPerTester, maxSleepTime, redshiftJdbcTemplate));
                break;
            }
        }
    }

    public String run() {
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
        } catch (InterruptedException|ExecutionException e) {
            log.error(e.getMessage(), e);
        }

        return getMetrics().toString();
    }

    public Map<LoadTestStatementType, Double> getMetrics() {
        Map<LoadTestStatementType, List<ResultMetrics>> all = new HashMap<>();
        for (AbstractLoadTest tester : testers) {
            all.put(tester.sqlStatementType, tester.getMetrics());
        }

        Map<LoadTestStatementType, Double> results = new HashMap<>();
        for (LoadTestStatementType statementType : all.keySet()) {
            double average = all.get(statementType).stream().mapToLong(x -> x.getRuntime()).average().getAsDouble();
            results.put(statementType, average);
        }
        return results;
    }
}
