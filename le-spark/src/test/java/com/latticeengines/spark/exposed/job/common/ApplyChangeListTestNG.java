package com.latticeengines.spark.exposed.job.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ApplyChangeListConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class ApplyChangeListTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testUpsert() {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(this::testMergeChangeList);
        runnables.add(this::testNewChangeList);
        ThreadPoolUtils.runInParallel(this.getClass().getSimpleName(), runnables);
    }

    private void testMergeChangeList() {
        List<String> input = upload2Data();
        ApplyChangeListConfig config = getConfigForChangeList();
        config.setHasSourceTbl(true);
        SparkJobResult result = runSparkJob(ApplyChangeListJob.class, config, input,
                String.format("/tmp/%s/%s/changeList", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyChangeList));
    }

    private void testNewChangeList() {
        List<String> input = upload1Data();
        ApplyChangeListConfig config = getConfigForChangeList();
        config.setHasSourceTbl(false);
        SparkJobResult result = runSparkJob(ApplyChangeListJob.class, config, input,
                String.format("/tmp/%s/%s/newChangeList", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyNewChangeList));
    }

    private List<String> upload1Data() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("RowId", String.class), //
                Pair.of("ColumnId", String.class), //
                Pair.of("DataType", String.class), //
                Pair.of("Deleted", Boolean.class), //
                Pair.of("FromString", String.class), //
                Pair.of("ToString", String.class), //
                Pair.of("FromInteger", Integer.class), //
                Pair.of("ToInteger", Integer.class) //
        );
        Object[][] data = getInput1Data();
        input.add(uploadHdfsDataUnit(data, fields));
        return input;
    }

    private List<String> upload2Data() {
        List<String> input = upload1Data();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("EntityId", String.class), //
                Pair.of("first", String.class), //
                Pair.of("last", String.class), //
                Pair.of("age", Integer.class), //
                Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class) //

        );
        Object[][] data = getInput2Data();
        input.add(uploadHdfsDataUnit(data, fields));
        Collections.reverse(input);
        return input;
    }

    private Object[][] getInput1Data() {
        return new Object[][] { //
                { 0, "entityId66", null, "String", true, null, null, null, null }, //
                { 1, "entityId5", null, "String", true, null, null, null, null }, //
                { 2, null, "first", "String", true, null, null, null, null }, //

                { 3, "entityId1", "first", "String", null, null, "john12", null, null }, //
                { 4, "entityId1", "last", "String", null, null, "smith12", null, null }, //
                { 5, "entityId2", "last", "String", null, "dummy", "ann22", null, null }, //
                { 6, "entityId2", "age", "Integer", null, null, null, -1, 22 }, //
                { 7, "entityId3", "age", "Integer", null, null, null, 3, 33 }, //
                { 8, "entityId4", "age2", "Integer", null, null, null, 4, 44 } //
        };
    }

    private Object[][] getInput2Data() {
        return new Object[][] { //
                { 1, "entityId1", "john", "smith", 11, 1000001L }, //
                { 2, "entityId2", "mary", "ann", 18, 1000002L }, //
                { 2, "entityId5", "mary5", "ann5", 55, 1000002L } //
        };
    }

    private ApplyChangeListConfig getConfigForChangeList() {
        ApplyChangeListConfig config = new ApplyChangeListConfig();
        config.setJoinKey("EntityId");
        return config;
    }

    private Boolean verifyChangeList(HdfsDataUnit tgt) {
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            Assert.assertEquals(record.getSchema().getFields().size(), 7, record.toString());

            String entityId = record.get("EntityId").toString();
            String first = record.get("first") != null ? record.get("first").toString() : null;
            String last = record.get("last") != null ? record.get("last").toString() : null;
            String age = record.get("age") != null ? record.get("age").toString() : null;
            String age2 = record.get("age2") != null ? record.get("age2").toString() : null;
            switch (entityId) {
            case "entityId1":
                Assert.assertEquals(first, "john12", record.toString());
                Assert.assertEquals(last, "smith12", record.toString());
                Assert.assertEquals(age, "11", record.toString());
                Assert.assertNull(age2, record.toString());
                break;
            case "entityId2":
                Assert.assertNull(first, record.toString());
                Assert.assertEquals(last, "ann22", record.toString());
                Assert.assertEquals(age, "22", record.toString());
                Assert.assertNull(age2, record.toString());
                break;
            case "entityId3":
                Assert.assertNull(first, record.toString());
                Assert.assertNull(last, record.toString());
                Assert.assertEquals(age, "33", record.toString());
                Assert.assertNull(age2, record.toString());
                break;
            case "entityId4":
                Assert.assertNull(first, record.toString());
                Assert.assertNull(last, record.toString());
                Assert.assertNull(age, record.toString());
                Assert.assertEquals(age2, "44", record.toString());
                break;
            default:
            }
            rows++;
        }
        Assert.assertEquals(rows, 4);
        return true;
    }

    private Boolean verifyNewChangeList(HdfsDataUnit tgt) {
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            Assert.assertEquals(record.getSchema().getFields().size(), 5, record.toString());

            String entityId = record.get("EntityId").toString();
            String first = record.get("first") != null ? record.get("first").toString() : null;
            String last = record.get("last") != null ? record.get("last").toString() : null;
            String age = record.get("age") != null ? record.get("age").toString() : null;
            String age2 = record.get("age2") != null ? record.get("age2").toString() : null;
            switch (entityId) {
            case "entityId1":
                Assert.assertEquals(first, "john12", record.toString());
                Assert.assertEquals(last, "smith12", record.toString());
                Assert.assertNull(age, record.toString());
                Assert.assertNull(age2, record.toString());
                break;
            case "entityId2":
                Assert.assertNull(first, record.toString());
                Assert.assertEquals(last, "ann22", record.toString());
                Assert.assertEquals(age, "22", record.toString());
                Assert.assertNull(age2, record.toString());
                break;
            case "entityId3":
                Assert.assertNull(first, record.toString());
                Assert.assertNull(last, record.toString());
                Assert.assertEquals(age, "33", record.toString());
                Assert.assertNull(age2, record.toString());
                break;
            case "entityId4":
                Assert.assertNull(first, record.toString());
                Assert.assertNull(last, record.toString());
                Assert.assertNull(age, record.toString());
                Assert.assertEquals(age2, "44", record.toString());
                break;
            default:
            }
            rows++;
        }
        Assert.assertEquals(rows, 4);
        return true;
    }
}
