package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.SparkIOMetadataWrapper;
import com.latticeengines.domain.exposed.spark.cdl.SplitTransactionConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class SplitTransactionJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SplitTransactionJobTestNG.class);

    private static final String productId = InterfaceName.ProductId.name();
    private static final String productType = InterfaceName.ProductType.name();
    private static final String accountId = InterfaceName.AccountId.name();
    private static final String contactId = InterfaceName.ContactId.name();
    private static final String txnId = InterfaceName.TransactionId.name();
    private static final String txnTime = InterfaceName.TransactionTime.name();
    private static final String txnType = InterfaceName.TransactionType.name();
    private static final String streamDate = InterfaceName.__StreamDate.name();
    private static final String streamDateId = InterfaceName.StreamDateId.name();

    private static final String spending = ProductType.Spending.name();
    private static final String analytic = ProductType.Analytic.name();
    private static final String bundle = ProductType.Bundle.name();

    private static final List<String> OUTPUT_FIELDS = Arrays.asList(txnId, txnType, accountId, contactId, productId, txnTime, streamDate, productType); // streamDateId used for partition
    private static final String DAY_1 = "2019-07-01";
    private static final String DAY_2 = "2019-07-02";
    private static final long DAY_1_EPOCH = 1561964400000L;
    private static final long DAY_2_EPOCH = 1562050900000L;
    private static final Integer DAY_1_ID = DateTimeUtils.dateToDayPeriod(DAY_1);
    private static final Integer DAY_2_ID = DateTimeUtils.dateToDayPeriod(DAY_2);


    @Test(groups = "functional")
    private void test() {
        List<String> inputs = Collections.singletonList(prepareTransactionTable());
        SplitTransactionConfig config = new SplitTransactionConfig();
        SparkJobResult result = runSparkJob(SplitTransactionJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
        SparkIOMetadataWrapper outputMetadata = JsonUtils.deserialize(result.getOutput(), SparkIOMetadataWrapper.class);
        Assert.assertEquals(outputMetadata.getMetadata().size(), 2);
        verify(result, Arrays.asList(this::verifyAnalytic, this::verifySpending));
    }

    @Test(groups = "functional")
    private void testRetainOneType() {
        List<String> inputs = Collections.singletonList(prepareTransactionTable());
        SplitTransactionConfig config = new SplitTransactionConfig();
        config.retainProductType = Collections.singletonList(analytic);
        SparkJobResult result = runSparkJob(SplitTransactionJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
        SparkIOMetadataWrapper outputMetadata = JsonUtils.deserialize(result.getOutput(), SparkIOMetadataWrapper.class);
        Assert.assertEquals(outputMetadata.getMetadata().size(), 1);
        verify(result, Collections.singletonList(this::verifyAnalytic));
    }

    @Test(groups = "functional")
    private void testNoTxnType() {
        List<String> inputs = Collections.singletonList(prepareTransactionTableWIthNoTxnType());
        SplitTransactionConfig config = new SplitTransactionConfig();
        SparkJobResult result = runSparkJob(SplitTransactionJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
        SparkIOMetadataWrapper outputMetadata = JsonUtils.deserialize(result.getOutput(), SparkIOMetadataWrapper.class);
        Assert.assertEquals(outputMetadata.getMetadata().size(), 2);
        verify(result, Arrays.asList(this::verifyAnalytic, this::verifySpending));
    }

    @Test(groups = "functional")
    private void testNullTxnType() {
        List<String> inputs = Collections.singletonList(prepareTransactionTableWithNullTxnType());
        SplitTransactionConfig config = new SplitTransactionConfig();
        SparkJobResult result = runSparkJob(SplitTransactionJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
        SparkIOMetadataWrapper outputMetadata = JsonUtils.deserialize(result.getOutput(), SparkIOMetadataWrapper.class);
        Assert.assertEquals(outputMetadata.getMetadata().size(), 2);
        verify(result, Arrays.asList(this::verifyAnalytic, this::verifySpending));
    }

    @Test(groups = "functional")
    private void testAllNullTxnType() {
        List<String> inputs = Collections.singletonList(prepareTransactionTableWIthAllNullTxnType());
        SplitTransactionConfig config = new SplitTransactionConfig();
        SparkJobResult result = runSparkJob(SplitTransactionJob.class, config, inputs, getWorkspace());
        log.info("Output metadata: {}", result.getOutput());
        SparkIOMetadataWrapper outputMetadata = JsonUtils.deserialize(result.getOutput(), SparkIOMetadataWrapper.class);
        Assert.assertEquals(outputMetadata.getMetadata().size(), 2);
        verify(result, Arrays.asList(this::verifyAnalytic, this::verifySpending));
    }

    private String prepareTransactionTable() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(txnId, String.class),
                Pair.of(txnType, String.class),
                Pair.of(accountId, String.class),
                Pair.of(contactId, String.class),
                Pair.of(productId, String.class),
                Pair.of(txnTime, Long.class),
                Pair.of(productType, String.class)
        );
        Object[][] data = new Object[][]{
                {"t1", "Purchase", "a1", "c1", "p1", DAY_1_EPOCH, spending},
                {"t2", "Purchase", "a1", "c1", "p2", DAY_2_EPOCH, analytic},
                {"t3", "Purchase", "a1", "c2", "p2", DAY_2_EPOCH, analytic},
                {"t4", "Purchase", "a2", "c3", "p3", DAY_1_EPOCH, spending},
                {"t5", "Purchase", "a2", "c4", "p3", DAY_1_EPOCH, spending},
                {"t6", "Purchase", "a2", "c5", "p4", DAY_2_EPOCH, bundle}
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String prepareTransactionTableWIthNoTxnType() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(txnId, String.class),
                Pair.of(accountId, String.class),
                Pair.of(contactId, String.class),
                Pair.of(productId, String.class),
                Pair.of(txnTime, Long.class),
                Pair.of(productType, String.class)
        );
        Object[][] data = new Object[][]{
                {"t1", "a1", "c1", "p1", DAY_1_EPOCH, spending},
                {"t2", "a1", "c1", "p2", DAY_2_EPOCH, analytic},
                {"t3", "a1", "c2", "p2", DAY_2_EPOCH, analytic},
                {"t4", "a2", "c3", "p3", DAY_1_EPOCH, spending},
                {"t5", "a2", "c4", "p3", DAY_1_EPOCH, spending},
                {"t6", "a2", "c5", "p4", DAY_2_EPOCH, bundle}
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String prepareTransactionTableWithNullTxnType() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(txnId, String.class),
                Pair.of(txnType, String.class),
                Pair.of(accountId, String.class),
                Pair.of(contactId, String.class),
                Pair.of(productId, String.class),
                Pair.of(txnTime, Long.class),
                Pair.of(productType, String.class)
        );
        Object[][] data = new Object[][]{
                {"t1", "Purchase", "a1", "c1", "p1", DAY_1_EPOCH, spending},
                {"t2", null, "a1", "c1", "p2", DAY_2_EPOCH, analytic},
                {"t3", "Purchase", "a1", "c2", "p2", DAY_2_EPOCH, analytic},
                {"t4", "Purchase", "a2", "c3", "p3", DAY_1_EPOCH, spending},
                {"t5", null, "a2", "c4", "p3", DAY_1_EPOCH, spending},
                {"t6", "Purchase", "a2", "c5", "p4", DAY_2_EPOCH, bundle}
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String prepareTransactionTableWIthAllNullTxnType() {
        List<Pair<String, Class<?>>> fields = Arrays.asList(
                Pair.of(txnId, String.class),
                Pair.of(txnType, String.class),
                Pair.of(accountId, String.class),
                Pair.of(contactId, String.class),
                Pair.of(productId, String.class),
                Pair.of(txnTime, Long.class),
                Pair.of(productType, String.class)
        );
        Object[][] data = new Object[][]{
                {"t1", null, "a1", "c1", "p1", DAY_1_EPOCH, spending},
                {"t2", null, "a1", "c1", "p2", DAY_2_EPOCH, analytic},
                {"t3", null, "a1", "c2", "p2", DAY_2_EPOCH, analytic},
                {"t4", null, "a2", "c3", "p3", DAY_1_EPOCH, spending},
                {"t5", null, "a2", "c4", "p3", DAY_1_EPOCH, spending},
                {"t6", null, "a2", "c5", "p4", DAY_2_EPOCH, bundle}
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private Boolean verifySpending(HdfsDataUnit df) {
        Object[][] expectedResult = new Object[][] {
                {"t1", "Purchase", "a1", "c1", "p1", DAY_1_EPOCH, DAY_1, spending},
                {"t4", "Purchase", "a2", "c3", "p3", DAY_1_EPOCH, DAY_1, spending},
                {"t5", "Purchase", "a2", "c4", "p3", DAY_1_EPOCH, DAY_1, spending}
        };
        Map<String, List<Pair<String, String>>> expectedMap = constructExpectedMap(OUTPUT_FIELDS, expectedResult);
        Iterator<GenericRecord> iterator = verifyAndReadTarget(df);
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            List<Pair<String, String>> expectedData = expectedMap.get(record.get(txnId).toString());
            Assert.assertNotNull(expectedData);
            expectedData.forEach(data -> Assert.assertEquals(data.getValue(), record.get(data.getKey()).toString()));
        }
        return true;
    }

    private Boolean verifyAnalytic(HdfsDataUnit df) {
        Object[][] expectedResult = new Object[][] {
                {"t2", "Purchase", "a1", "c1", "p2", DAY_2_EPOCH, DAY_2, analytic},
                {"t3", "Purchase", "a1", "c2", "p2", DAY_2_EPOCH, DAY_2, analytic}
        };
        Map<String, List<Pair<String, String>>> expectedMap = constructExpectedMap(OUTPUT_FIELDS, expectedResult);
        Iterator<GenericRecord> iterator = verifyAndReadTarget(df);
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            List<Pair<String, String>> expectedData = expectedMap.get(record.get(txnId).toString());
            Assert.assertNotNull(expectedData);
            expectedData.forEach(data -> Assert.assertEquals(data.getValue(), record.get(data.getKey()).toString()));
        }
        return true;
    }

    // txnId -> [(field, val)]
    private Map<String, List<Pair<String, String>>> constructExpectedMap(List<String> fields, Object[][] result) {
        Map<String, List<Pair<String, String>>> map = new HashMap<>();
        for (Object[] row : result) {
            List<Pair<String, String>> data = new ArrayList<>();
            for (int i = 0; i < fields.size(); i++) {
                String field = fields.get(i);
                String val = row[i].toString();
                data.add(Pair.of(field, val));
            }
            map.put(row[0].toString(), data);
        }
        return map;
    }
}
