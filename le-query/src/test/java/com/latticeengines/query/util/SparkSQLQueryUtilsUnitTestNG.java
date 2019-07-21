package com.latticeengines.query.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SparkSQLQueryUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testDetachCTE() {
        for (String queryFile: Arrays.asList("query1", "query2", "query3")) {
            String sql = readSqlFromResource(queryFile);
            List<List<String>> queries = SparkSQLQueryUtils.detachSubQueries(sql);
            AtomicReference<String> segmentSqlRef = new AtomicReference<>();
            queries.forEach(l -> {
                String name = l.get(0);
                Assert.assertFalse(name.contains(" "), name);
                String statement = l.get(1);
                Assert.assertTrue(statement.startsWith("select") || statement.startsWith("("), statement);
                if (l.get(0).equals("segment")) {
                    segmentSqlRef.set(statement);
                }
                System.out.println("\n========== " + l.get(0) + " ==========");
                System.out.println(statement);
            });
        }
    }

    @Test(groups = "unit")
    public void testExtractCTE() {
        String sql = readSqlFromResource("query1-1");
        List<List<String>> queries = SparkSQLQueryUtils.extractSubQueries(sql, "segment");
        queries.forEach(l -> {
            String alias = l.get(0);
            String statement = l.get(1);
            System.out.println("\n========== " + alias + " ==========");
            System.out.println(statement);
        });
    }

    private String readSqlFromResource(String fileName) {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource resource = resolver.getResource("com/latticeengines/query/util/" + fileName);
        try {
            return IOUtils.toString(resource.getInputStream(), "utf-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
