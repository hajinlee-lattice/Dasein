package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MergeImportsTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MergeImportsTestNG.class);

    // All the schema should have AccountId field as row identifier for result
    // verification
    private static final String[] FIELDS1 = { InterfaceName.Id.name(), "AID1", "__template__" };
    private static final String[] FIELDS1_EXPECTED = { InterfaceName.Id.name(), "AID1", "__template__" };
    private static final String[] FIELDS3 = { InterfaceName.Id.name(), "AID1", "AID2" };
    private static final String[] FIELDS4 = { InterfaceName.Id.name(), "prefix__" + InterfaceName.AccountId.name(),
            "AID1", "AID2" };

    private static final String[] FIELDS5 = { InterfaceName.Id.name(), "AID1", "AID2", "Expect_Remove" };

    @Test(groups = "functional")
    public void test() {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(this::test1);
        runnables.add(this::test2);
        runnables.add(this::test3);
        runnables.add(this::test4);
        runnables.add(this::test5);
        ThreadPoolUtils.runInParallel(this.getClass().getSimpleName(), runnables);
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
        verify(result, Collections.singletonList(this::verifyTarget1));
    }

    private List<String> uploadDataTest1() {
        List<String> orderedInput = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = new ArrayList<>();
        for (String field : FIELDS1) {
            fields.add(Pair.of(field, String.class));
        }
        Object[][] data = new Object[][] { //
                { "1", "A1", "template1" }, //
                { "1", "A2", "template2" }, //
                { "2", "A2", "template1" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));

        data = new Object[][] { //
                { "3", "A1", "template2" }, //
                { "4", "A3", "template2" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));
        return orderedInput;
    }

    private String getWorkspace1() {
        return String.format("/tmp/%s/%s/Test1", leStack, this.getClass().getSimpleName());
    }

    private Boolean verifyTarget1(HdfsDataUnit tgt) {
        Object[][] expectedResult = new String[][] { //
                { "1", "A1", "template1" }, //
                { "1", "A2", "template2" }, //
                { "2", "A2", "template1" }, //
                { "3", "A1", "template2" }, //
                { "4", "A3", "template2" }, //
        };
        Map<String, List<Object>> expectedMap = Arrays.stream(expectedResult)
                .collect(Collectors.toMap(arr -> (String) arr[0] + "-" + (String) arr[2], Arrays::asList));
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            System.out.println(record);
            verifyTargetData(FIELDS1_EXPECTED, expectedMap, record, "__template__");
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
        config.setHasSystem(true);
        config.setRequiredColumns(ImmutableMap.of("Id1", "string", "Id2", "long"));
        SparkJobResult result = runSparkJob(MergeImportsJob.class, config, orderedInput, getWorkspace2());
        verify(result, Collections.singletonList(this::verifyTarget2));
    }

    private List<String> uploadDataTest2() {
        List<String> orderedInput = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = new ArrayList<>();
        for (String field : FIELDS1) {
            fields.add(Pair.of(field, String.class));
        }
        Object[][] data = new Object[][] { //
                { "1", "A1", "template1" }, //
                { "1", "A2", "template1" }, //
                { "1", "A3", "template3" }, //
                { "2", "A2", "template1" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));

        data = new Object[][] { //
                { "1", "A1", "template2" }, //
                { "3", "A3", "template2" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));
        return orderedInput;
    }

    private String getWorkspace2() {
        return String.format("/tmp/%s/%s/Test2", leStack, this.getClass().getSimpleName());
    }

    private Boolean verifyTarget2(HdfsDataUnit tgt) {
        Object[][] expectedResult = new String[][] { //
                { "1", "A2", "template1" }, //
                { "1", "A3", "template3" }, //
                { "1", "A1", "template2" }, //
                { "2", "A2", "template1" }, //
                { "3", "A3", "template2" }, //
        };
        Map<String, List<Object>> expectedMap = Arrays.stream(expectedResult)
                .collect(Collectors.toMap(arr -> (String) arr[0] + "-" + (String) arr[2], Arrays::asList));
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            verifyTargetData(FIELDS1_EXPECTED, expectedMap, record, "__template__");
            Schema schema = record.getSchema();
            verifyFieldOfType(schema, "Id1", String.class);
            verifyFieldOfType(schema, "Id2", Long.class);
            Assert.assertNotNull(record.get(InterfaceName.CDLCreatedTime.name()));
            Assert.assertNotNull(record.get(InterfaceName.CDLUpdatedTime.name()));
            rows++;
        }
        Assert.assertEquals(rows, expectedResult.length);
        return true;
    }

    private void verifyFieldOfType(Schema schema, String field, Class<?> type) {
        Schema.Field avroField = schema.getField(field);
        Assert.assertNotNull(avroField);
        Class<?> javaClz = AvroUtils.getJavaType(AvroUtils.getType(avroField));
        Assert.assertEquals(javaClz, type);
    }

    // Test concat imports -- with column rename and clone
    private void test3() {
        List<String> orderedInput = uploadDataTest3();
        log.info("Inputs for test3: {}", String.join(",", orderedInput));

        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(false);
        config.setJoinKey(null);
        config.setAddTimestamps(false);
        config.setCloneSrcFields(new String[][] { //
                // Copy AID1 to AID1_COPY
                { "AID1", "AID1_COPY" },
                // Cannot copy NON_EXISTS1 to NON_EXISTS1_COPY
                { "NON_EXISTS1", "NON_EXISTS1_COPY" } });
        config.setRenameSrcFields(new String[][] { //
                // Rename AID1 to AID1_NEW
                { "AID1", "AID1_NEW" }, //
                // Cannot rename NON_EXISTS2 to NON_EXISTS2_NEW
                { "NON_EXISTS2", "NON_EXISTS2_NEW" },
                // Cannot rename due to AccountId already exists
                { "AID2", InterfaceName.Id.name() } });
        SparkJobResult result = runSparkJob(MergeImportsJob.class, config, orderedInput, getWorkspace3());
        verify(result, Collections.singletonList(this::verifyTarget3));
    }

    private List<String> uploadDataTest3() {
        List<String> orderedInput = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = new ArrayList<>();
        for (String field : FIELDS3) {
            fields.add(Pair.of(field, String.class));
        }
        Object[][] data = new Object[][] { //
                { "1", "A1", "B1" }, //
                { "2", "A2", "B2" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));

        data = new Object[][] { //
                { "3", "A1", "B1" }, //
                { "4", "A3", "B3" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));
        return orderedInput;
    }

    private String getWorkspace3() {
        return String.format("/tmp/%s/%s/Test3", leStack, this.getClass().getSimpleName());
    }

    private Boolean verifyTarget3(HdfsDataUnit tgt) {
        // Id, AID1_COPY, AID1_NEW, AID2
        Object[][] expectedResult = new String[][] { //
                { "1", "A1", "A1", "B1" }, //
                { "2", "A2", "A2", "B2" }, //
                { "3", "A1", "A1", "B1" }, //
                { "4", "A3", "A3", "B3" }, //
        };
        Map<String, List<Object>> expectedMap = Arrays.stream(expectedResult)
                .collect(Collectors.toMap(arr -> (String) arr[0], Arrays::asList));
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            String[] expectedFlds = { InterfaceName.Id.name(), "AID1_COPY", "AID1_NEW", "AID2" };
            verifyTargetData(expectedFlds, expectedMap, record, "");
            rows++;
        }
        Assert.assertEquals(rows, expectedResult.length);
        return true;
    }

    // Test
    // 1: dedup imports -- not overwrite by null
    // 2: Id columns with a template prefix and ANONYMOUS value
    private void test4() {
        List<String> orderedInput = uploadDataTest4();
        log.info("Inputs for test4: {}", String.join(",", orderedInput));

        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(true);
        config.setJoinKey(InterfaceName.Id.name());
        SparkJobResult result = runSparkJob(MergeImportsJob.class, config, orderedInput, getWorkspace4());
        verify(result, Collections.singletonList(this::verifyTarget4));
    }

    private List<String> uploadDataTest4() {
        List<String> orderedInput = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = new ArrayList<>();
        // Test merge with columns with different types
        for (int i = 0; i < FIELDS4.length - 1; i++) {
            fields.add(Pair.of(FIELDS4[i], String.class));
        }
        fields.add(Pair.of(FIELDS4[FIELDS4.length - 1], Integer.class));
        // Id, AccountId, AID1, AID2
        Object[][] data = new Object[][] { //
                // dedup within single import
                { "1", "A", "B", 1 }, //
                { "1", "A", "B", 1 }, //

                { "2", "A", "B", 2 }, //
                { "2", DataCloudConstants.ENTITY_ANONYMOUS_ID, null, null }, //

                { "3", DataCloudConstants.ENTITY_ANONYMOUS_ID, null, null }, //
                { "3", "A", "B", 3 }, //

                { "4", DataCloudConstants.ENTITY_ANONYMOUS_ID, null, 4 }, //
                { "4", "A", null, null }, //
                { "4", DataCloudConstants.ENTITY_ANONYMOUS_ID, null, 4 }, //
                { "4", null, "B", null }, //

                { "5", DataCloudConstants.ENTITY_ANONYMOUS_ID, null, null }, //
                { "5", null, null, null }, //

                // Dedup across multi imports
                { "6", "A", null, 6 }, //
                { "6", DataCloudConstants.ENTITY_ANONYMOUS_ID, "B", null }, //

                { "7", DataCloudConstants.ENTITY_ANONYMOUS_ID, "B", 7 }, //
                { "7", DataCloudConstants.ENTITY_ANONYMOUS_ID, "B", 7 }, //

                { "8", DataCloudConstants.ENTITY_ANONYMOUS_ID, null, null }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));

        data = new Object[][] { //
                // Dedup across multi imports, test ANONYMOUS for non-id column
                { "6", null, null, null }, //
                { "6", DataCloudConstants.ENTITY_ANONYMOUS_ID, DataCloudConstants.ENTITY_ANONYMOUS_ID, null }, //

                { "7", null, "B", null }, //
                { "7", "A", null, 7 }, //

                // test ANONYMOUS for non-id column
                { "8", null, DataCloudConstants.ENTITY_ANONYMOUS_ID, 8 }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));
        return orderedInput;
    }

    private String getWorkspace4() {
        return String.format("/tmp/%s/%s/Test4", leStack, this.getClass().getSimpleName());
    }

    private Boolean verifyTarget4(HdfsDataUnit tgt) {
        // Id, AccountId, AID1, AID2
        Object[][] expectedResult = new Object[][] { //
                { "1", "A", "B", 1 }, //
                { "2", "A", "B", 2 }, //
                { "3", "A", "B", 3 }, //
                { "4", "A", "B", 4 }, //
                { "5", DataCloudConstants.ENTITY_ANONYMOUS_ID, null, null }, //
                { "6", "A", DataCloudConstants.ENTITY_ANONYMOUS_ID, 6 }, //
                { "7", "A", "B", 7 }, //
                { "8", DataCloudConstants.ENTITY_ANONYMOUS_ID, DataCloudConstants.ENTITY_ANONYMOUS_ID, 8 }, //
        };
        Map<String, List<Object>> expectedMap = Arrays.stream(expectedResult)
                .collect(Collectors.toMap(arr -> (String) arr[0], Arrays::asList));
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            verifyTargetData(FIELDS4, expectedMap, record, "");
            rows++;
        }
        Assert.assertEquals(rows, expectedResult.length);
        return true;
    }

    /**
     * Test Exclude attribute
     */
    private void test5() {
        List<String> orderedInput = uploadDataTest5();
        log.info("Inputs for test3: {}", String.join(",", orderedInput));

        MergeImportsConfig config = new MergeImportsConfig();
        config.setDedupSrc(false);
        config.setJoinKey(null);
        config.setAddTimestamps(false);
        config.setCloneSrcFields(new String[][] { //
                // Copy AID1 to AID1_COPY
                { "AID1", "AID1_COPY" },
                // Cannot copy NON_EXISTS1 to NON_EXISTS1_COPY
                { "NON_EXISTS1", "NON_EXISTS1_COPY" } });
        config.setRenameSrcFields(new String[][] { //
                // Rename AID1 to AID1_NEW
                { "AID1", "AID1_NEW" }, //
                // Cannot rename NON_EXISTS2 to NON_EXISTS2_NEW
                { "NON_EXISTS2", "NON_EXISTS2_NEW" },
                // Cannot rename due to AccountId already exists
                { "AID2", InterfaceName.Id.name() } });
        config.setExcludeAttrs(Collections.singletonList("Expect_Remove"));
        SparkJobResult result = runSparkJob(MergeImportsJob.class, config, orderedInput, getWorkspace5());
        verify(result, Collections.singletonList(this::verifyTarget5));
    }

    private List<String> uploadDataTest5() {
        List<String> orderedInput = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = new ArrayList<>();
        for (String field : FIELDS5) {
            fields.add(Pair.of(field, String.class));
        }
        Object[][] data = new Object[][] { //
                { "1", "A1", "B1", "R1" }, //
                { "2", "A2", "B2", "R2" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));

        data = new Object[][] { //
                { "3", "A1", "B1", "R3" }, //
                { "4", "A3", "B3", "R4" }, //
        };
        orderedInput.add(uploadHdfsDataUnit(data, fields));
        return orderedInput;
    }

    private String getWorkspace5() {
        return String.format("/tmp/%s/%s/Test5", leStack, this.getClass().getSimpleName());
    }

    private Boolean verifyTarget5(HdfsDataUnit tgt) {
        // Id, AID1_COPY, AID1_NEW, AID2
        Object[][] expectedResult = new String[][] { //
                { "1", "A1", "A1", "B1" }, //
                { "2", "A2", "A2", "B2" }, //
                { "3", "A1", "A1", "B1" }, //
                { "4", "A3", "A3", "B3" }, //
        };
        Map<String, List<Object>> expectedMap = Arrays.stream(expectedResult)
                .collect(Collectors.toMap(arr -> (String) arr[0], Arrays::asList));
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            String[] expectedFlds = { InterfaceName.Id.name(), "AID1_COPY", "AID1_NEW", "AID2" };
            Assert.assertNull(record.getSchema().getField("Expect_Remove"));
            verifyTargetData(expectedFlds, expectedMap, record, "");
            rows++;
        }
        Assert.assertEquals(rows, expectedResult.length);
        return true;
    }


    /******************
     * Shared methods
     ******************/

    private void verifyTargetData(String[] fields, Map<String, List<Object>> expectedMap, GenericRecord record,
            String template) {
        log.info(record.toString());
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.get(InterfaceName.Id.name()));
        String id = record.get(InterfaceName.Id.name()).toString();
        if (StringUtils.isNotBlank(template)) {
            String templateName = record.get(template).toString();
            id = id + "-" + templateName;
        }
        List<Object> expected = expectedMap.get(id);
        Assert.assertNotNull(expected);
        List<Object> actual = Arrays.stream(fields)
                .map(field -> record.get(field) == null ? null
                        : (record.get(field) instanceof Utf8 ? record.get(field).toString() : record.get(field)))
                .collect(Collectors.toList());
        Assert.assertEquals(actual, expected);
    }
}
