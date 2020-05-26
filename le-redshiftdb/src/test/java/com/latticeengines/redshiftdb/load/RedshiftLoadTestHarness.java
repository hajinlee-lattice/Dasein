package com.latticeengines.redshiftdb.load;

import java.util.ArrayList;
import java.util.Collections;
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

    private static final Logger log = LoggerFactory.getLogger(RedshiftLoadTestHarness.class);

    private List<AbstractLoadTest> testers = new ArrayList<>();

    public RedshiftLoadTestHarness(int load, int stmtsPerTester, Set<LoadTestStatementType> possibleStmtTypes,
            int maxSleepTime, JdbcTemplate jdbcTemplate) {

        // setup testers as per the config
        Random random = new Random();
        if (possibleStmtTypes.size() < 1)
            possibleStmtTypes = new HashSet<>(Collections.singletonList(LoadTestStatementType.Query_Numeric_Join));

        LoadTestStatementType[] typesArray = possibleStmtTypes.toArray(new LoadTestStatementType[0]);

        for (int i = 0; i < load; ++i) {
            LoadTestStatementType statementType = typesArray[random.nextInt(possibleStmtTypes.size())];
            switch (statementType.getTesterType()) {
            case "QueryLoadTest":
                testers.add(new QueryLoadTest(i, stmtsPerTester, maxSleepTime, jdbcTemplate, statementType));
                break;

            case "AlterDDLLoadTest":
                testers.add(new AlterDDLLoadTest(i, stmtsPerTester, maxSleepTime, jdbcTemplate));
                break;

            case "QueryMultiConditionalTest":
                testers.add(new MultiConditionalQueryLoadTest(i, stmtsPerTester, maxSleepTime, jdbcTemplate));
                break;

            default:
            }
        }
    }

    public String run() {
        int loadLevel = 1;
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
