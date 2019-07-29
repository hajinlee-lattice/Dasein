package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MergeImportsTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MergeImportsTestNG.class);

    private ExecutorService workers;

    // All the schema should have Id field as row identifier for result
    // verification
    private static final String[] FIELDS1 = { InterfaceName.Id.name(), InterfaceName.AccountId.name() };
    private static final String[] FIELDS3 = { InterfaceName.Id.name(), InterfaceName.AccountId.name(), "AID" };

    @Test(groups = "functional")
    public void test() {
        workers = ThreadPoolUtils.getFixedSizeThreadPool("merge-imports-test", 2);

        List<Runnable> runnables = new ArrayList<>();
        Runnable runnable1 = () -> test1();
        runnables.add(runnable1);
        Runnable runnable2 = () -> test2();
        runnables.add(runnable2);
        Runnable runnable3 = () -> test3();
        runnables.add(runnable3);

        ThreadPoolUtils.runRunnablesInParallel(workers, runnables, 60, 1);

        workers.shutdownNow();
    }

    // Test concat imports -- very basic test case
    private void test1() {
        List<String> orderedInput = uploadDataTest1();
        log.info("Inputs for test1: {}", String.join(",", orderedInput));
        
        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(false);
        config.setJoinKey(null);
        config.setAddTimestamps(false);
        SparkJobResult result = runSparkJob(MergeImportsJob.class, config, orderedInput, getWorkspace1());
        verifyResult(result, Collections.singletonList(this::verifyTarget1));
    }

    private List<String> uploadDataTest1() {
        List<String> orderedInput = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = new ArrayList<>();
        for (String field : FIELDS1) {
            fields.add(Pair.of(field, String.class));
        }
        Object[][] data = new Object[][] { //
                { "1", "A1" }, //
                { "2", "A2" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));

        data = new Object[][] { //
                { "3", "A1" }, //
                { "4", "A3" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));
        return orderedInput;
    }

    private String getWorkspace1() {
        return String.format("/tmp/%s/%s/Test1", leStack, this.getClass().getSimpleName());
    }

    private Boolean verifyTarget1(HdfsDataUnit tgt) {
        String[][] expectedResult = new String[][] { //
                { "1", "A1" }, //
                { "2", "A2" }, //
                { "3", "A1" }, //
                { "4", "A3" }, //
        };
        Map<String, List<String>> expectedMap = Arrays.stream(expectedResult)
                .collect(Collectors.toMap(arr -> arr[0], arr -> Arrays.asList(arr)));
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            verifyTargetData(FIELDS1, expectedMap, record);
            rows++;
        }
        Assert.assertEquals(rows, expectedResult.length);
        return true;
    }

    // Test merge and dedup imports + adding timestamp
    private void test2() {
        List<String> orderedInput = uploadDataTest2();
        log.info("Inputs for test2: {}", String.join(",", orderedInput));
        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(true);
        config.setJoinKey(InterfaceName.Id.name());
        config.setAddTimestamps(true);
        SparkJobResult result = runSparkJob(MergeImportsJob.class, config, orderedInput, getWorkspace2());
        verifyResult(result, Collections.singletonList(this::verifyTarget2));
    }

    private List<String> uploadDataTest2() {
        List<String> orderedInput = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = new ArrayList<>();
        for (String field : FIELDS1) {
            fields.add(Pair.of(field, String.class));
        }
        Object[][] data = new Object[][] { //
                { "1", "A1" }, //
                { "2", "A2" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));

        data = new Object[][] { //
                { "1", "A1" }, //
                { "3", "A3" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));
        return orderedInput;
    }

    private String getWorkspace2() {
        return String.format("/tmp/%s/%s/Test2", leStack, this.getClass().getSimpleName());
    }

    private Boolean verifyTarget2(HdfsDataUnit tgt) {
        String[][] expectedResult = new String[][] { //
                { "1", "A1" }, //
                { "2", "A2" }, //
                { "3", "A3" }, //
        };
        Map<String, List<String>> expectedMap = Arrays.stream(expectedResult)
                .collect(Collectors.toMap(arr -> arr[0], arr -> Arrays.asList(arr)));
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            verifyTargetData(FIELDS1, expectedMap, record);
            Assert.assertNotNull(record.get(InterfaceName.CDLCreatedTime.name()));
            Assert.assertNotNull(record.get(InterfaceName.CDLUpdatedTime.name()));
            rows++;
        }
        Assert.assertEquals(rows, expectedResult.length);
        return true;
    }

    // Test concat imports -- with column rename and clone
    private void test3() {
        // input data same as test1
        List<String> orderedInput = uploadDataTest3();
        log.info("Inputs for test3: {}", String.join(",", orderedInput));

        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(false);
        config.setJoinKey(null);
        config.setAddTimestamps(false);
        config.setCloneSrcFields(
                new String[][] { //
                        // Copy AccountId to AID1
                        { InterfaceName.AccountId.name(), "AID1" }, 
                        // Cannot copy NON_EXISTS1 to AID3
                        { "NON_EXISTS1", "AID3" } });
        config.setRenameSrcFields(
                new String[][] { //
                        // Copy AccountId to AID2
                        { InterfaceName.AccountId.name(), "AID2" }, //
                        // Cannot copy NON_EXISTS2 to AID4
                        { "NON_EXISTS2", "AID4" },
                        // Cannot rename due to Id already exists
                        { "AID", InterfaceName.Id.name() } });
        SparkJobResult result = runSparkJob(MergeImportsJob.class, config, orderedInput, getWorkspace3());
        verifyResult(result, Collections.singletonList(this::verifyTarget3));
    }

    private List<String> uploadDataTest3() {
        List<String> orderedInput = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = new ArrayList<>();
        for (String field : FIELDS3) {
            fields.add(Pair.of(field, String.class));
        }
        Object[][] data = new Object[][] { //
                { "1", "A1", "AID1" }, //
                { "2", "A2", "AID2" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));

        data = new Object[][] { //
                { "3", "A1", "AID1" }, //
                { "4", "A3", "AID3" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));
        return orderedInput;
    }

    private String getWorkspace3() {
        return String.format("/tmp/%s/%s/Test3", leStack, this.getClass().getSimpleName());
    }

    private Boolean verifyTarget3(HdfsDataUnit tgt) {
        // Id, AID1, AID2
        String[][] expectedResult = new String[][] { //
                { "1", "A1", "A1" }, //
                { "2", "A2", "A2" }, //
                { "3", "A1", "A1" }, //
                { "4", "A3", "A3" }, //
        };
        Map<String, List<String>> expectedMap = Arrays.stream(expectedResult)
                .collect(Collectors.toMap(arr -> arr[0], arr -> Arrays.asList(arr)));
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            String[] expectedFlds = { InterfaceName.Id.name(), "AID1", "AID2" };
            verifyTargetData(expectedFlds, expectedMap, record);
            rows++;
        }
        Assert.assertEquals(rows, expectedResult.length);
        return true;
    }

    /******************
     * Shared methods
     ******************/

    private void verifyTargetData(String[] fields, Map<String, List<String>> expectedMap, GenericRecord record) {
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.get(InterfaceName.Id.name()));
        String id = record.get(InterfaceName.Id.name()).toString();
        List<String> expected = expectedMap.get(id);
        Assert.assertNotNull(expected);
        List<String> actual = Arrays.stream(fields)
                .map(field -> record.get(field) == null ? null : record.get(field).toString())
                .collect(Collectors.toList());
        Assert.assertEquals(actual, expected);
    }
}
