package com.latticeengines.spark.job;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.spark.ParseMatchResultJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class TestConcatJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TestConcatJobTestNG.class);

    private static final String STR_COL = "col_str";
    private static final String INTEGER_COL = "col_int";
    private static final String BOOLEAN_COL = "col_bool";

    @Test(groups = "functional")
    private void testOutOfOrderColumns() {
        SparkJobConfig config = prepareInput();
        SparkJobResult result = runSparkJob(TestConcatJob.class, config);
        verifyResult(result);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        AtomicInteger count = new AtomicInteger();
        Set<Integer> intVals = new HashSet<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            int row = count.incrementAndGet();
            Assert.assertNotNull(record.get(STR_COL));
            Assert.assertTrue(record.get(INTEGER_COL) instanceof Integer,
                    String.format("col %s should be a subclass of Integer", INTEGER_COL));
            Assert.assertTrue(record.get(BOOLEAN_COL) instanceof Boolean,
                    String.format("col %s should be a subclass of Boolean", BOOLEAN_COL));

            String strVal = record.get(STR_COL) == null ? null : record.get(STR_COL).toString();
            Integer intVal = (Integer) record.get(INTEGER_COL);
            Boolean boolVal = (Boolean) record.get(BOOLEAN_COL);
            log.info("Row={}: strVal={}, intVal={}, boolVal={}", row, strVal, intVal, boolVal);

            // check value based on intVal
            intVals.add(intVal);
            Assert.assertEquals(strVal, strVal(intVal));
            Assert.assertEquals(boolVal, boolVal(intVal));
        });
        Assert.assertEquals(count.get(), 6);
        Assert.assertEquals(intVals, new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6)));
        return true;
    }

    private SparkJobConfig prepareInput() {
        SparkJobConfig config = new ParseMatchResultJobConfig();

        // [ intCol, strCol, boolCol ]
        List<Pair<String, Class<?>>> fields1 = Arrays.asList( //
                Pair.of(INTEGER_COL, Integer.class), //
                Pair.of(STR_COL, String.class), //
                Pair.of(BOOLEAN_COL, Boolean.class) //
        );
        Object[][] data1 = new Object[][] { //
                { 1, strVal(1), boolVal(1) }, //
                { 2, strVal(2), boolVal(2) }, //
                { 5, strVal(5), boolVal(5) }, //
                { 6, strVal(6), boolVal(6) }, //
        };

        /*-
         * [ strCol, boolCol, intCol ]
         * column names are the same as fields1, but order is different
         * note that the second column is intentionally has type bool vs string (incompatible types)
         */
        List<Pair<String, Class<?>>> fields2 = Arrays.asList( //
                Pair.of(STR_COL, String.class), //
                Pair.of(BOOLEAN_COL, Boolean.class), //
                Pair.of(INTEGER_COL, Integer.class) //
        );
        Object[][] data2 = new Object[][] { //
                { strVal(3), boolVal(3), 3 }, //
                { strVal(4), boolVal(4), 4 }, //
        };
        uploadHdfsDataUnit(data1, fields1);
        uploadHdfsDataUnit(data2, fields2);
        return config;
    }

    private String strVal(int intVal) {
        return String.format("str_%d", intVal);
    }

    private Boolean boolVal(int intVal) {
        return intVal % 2 == 0;
    }
}
