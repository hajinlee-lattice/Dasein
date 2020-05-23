package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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
import com.latticeengines.domain.exposed.spark.cdl.ChangeListConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CreateChangeListTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CreateChangeListTestNG.class);

    @Test(groups = "functional")
    public void testUpsert() {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(this::testNewTable);
        runnables.add(this::testCreateChangeList);
        runnables.add(this::testNoChange);
        ThreadPoolUtils.runInParallel(this.getClass().getSimpleName(), runnables);
    }

    private void testNewTable() {
        List<String> input = upload1Data();
        ChangeListConfig config = getConfigForNewTable();
        SparkJobResult result = runSparkJob(CreateChangeListJob.class, config, input,
                String.format("/tmp/%s/%s/newTable", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyNewTable));
    }

    private void testCreateChangeList() {
        List<String> input = upload2Data();
        ChangeListConfig config = getConfigForChangeList();
        SparkJobResult result = runSparkJob(CreateChangeListJob.class, config, input,
                String.format("/tmp/%s/%s/changeList", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyChangeList));
    }

    private void testNoChange() {
        List<String> input = upload3Data();
        ChangeListConfig config = getConfigForNewTable();
        SparkJobResult result = runSparkJob(CreateChangeListJob.class, config, input,
                String.format("/tmp/%s/%s/noChange", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyNoChange));
    }

    private List<String> upload1Data() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("EntityId", String.class), //
                Pair.of("first", String.class), //
                Pair.of("last", String.class), //
                Pair.of("age", Integer.class), //
                Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class) //

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
                Pair.of("salary", Integer.class), //
                Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class) //

        );
        Object[][] data = getInput2Data();
        input.add(uploadHdfsDataUnit(data, fields));
        return input;
    }

    private List<String> upload3Data() {
        List<String> input1 = upload1Data();
        List<String> input2 = upload1Data();
        input1.addAll(input2);
        return input1;
    }

    private Object[][] getInput1Data() {
        Object[][] data = new Object[][] { //
                { 1, "entityId1", "john", "smith", null, 1000001L }, //
                { 2, "entityId2", null, "ann", 18, 1000002L } //
        };
        return data;
    }

    private Object[][] getInput2Data() {
        Object[][] data = new Object[][] { //
                { 1, "entityId1", "bill", null, 2000, 1000001L }, //
                { 3, "entityId3", "mary2", "ann2", null, 1000003L } //
        };
        return data;
    }

    private ChangeListConfig getConfigForNewTable() {
        ChangeListConfig config = new ChangeListConfig();
        config.setJoinKey("EntityId");
        config.setExclusionColumns(Arrays.asList("Id", "EntityId", InterfaceName.CDLCreatedTime.name()));
        return config;
    }

    private ChangeListConfig getConfigForChangeList() {
        ChangeListConfig config = new ChangeListConfig();
        config.setJoinKey("EntityId");
        config.setExclusionColumns(Arrays.asList("Id", InterfaceName.CDLCreatedTime.name()));
        return config;
    }

    private Boolean verifyNewTable(HdfsDataUnit tgt) {
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            Assert.assertEquals(record.getSchema().getFields().size(), 16, record.toString());
            String rowId = record.get("RowId") != null ? record.get("RowId").toString() : "#";
            String columnId = record.get("ColumnId") != null ? record.get("ColumnId").toString() : "#";
            String dataType = record.get("DataType") != null ? record.get("DataType").toString() : null;
            String isDeleted = record.get("Deleted") == null ? null : record.get("Deleted").toString();
            String fromString = record.get("FromString") == null ? null : record.get("FromString").toString();
            String toString = record.get("ToString") == null ? null : record.get("ToString").toString();
            String fromBoolean = record.get("FromBoolean") == null ? null : record.get("FromBoolean").toString();
            String toBoolean = record.get("ToBoolean") == null ? null : record.get("ToBoolean").toString();
            String fromInteger = record.get("FromInteger") == null ? null : record.get("FromInteger").toString();
            String toInteger = record.get("ToInteger") == null ? null : record.get("ToInteger").toString();
            String fromFloat = record.get("FromFloat") == null ? null : record.get("FromFloat").toString();
            String toFloat = record.get("ToFloat") == null ? null : record.get("ToFloat").toString();
            String FromDouble = record.get("FromDouble") == null ? null : record.get("FromDouble").toString();
            String toDouble = record.get("ToDouble") == null ? null : record.get("ToDouble").toString();

            String key = rowId + "-" + columnId;
            log.info("Key=" + key + " Record=" + record.toString());
            switch (key) {
            case "entityId1-#":
            case "entityId2-#":
                assertListNull(Arrays.asList(dataType, isDeleted, fromString, toString, fromBoolean, toBoolean,
                        fromInteger, toInteger, fromBoolean, toBoolean, fromFloat, toFloat, FromDouble, toDouble),
                        record);
                break;
            case "entityId1-first":
                Assert.assertEquals(dataType, "String", record.toString());
                Assert.assertEquals(toString, "john", record.toString());
                assertListNull(Arrays.asList(isDeleted, fromString, fromBoolean, toBoolean, fromInteger, toInteger,
                        fromBoolean, toBoolean, fromFloat, toFloat, FromDouble, toDouble), record);
                break;
            case "entityId1-last":
                Assert.assertEquals(dataType, "String", record.toString());
                Assert.assertEquals(toString, "smith", record.toString());
                assertListNull(Arrays.asList(isDeleted, fromString, fromBoolean, toBoolean, fromInteger, toInteger,
                        fromBoolean, toBoolean, fromFloat, toFloat, FromDouble, toDouble), record);
                break;
            case "entityId2-last":
                Assert.assertEquals(dataType, "String", record.toString());
                Assert.assertEquals(toString, "ann", record.toString());
                assertListNull(Arrays.asList(isDeleted, fromString, fromBoolean, toBoolean, fromInteger, toInteger,
                        fromBoolean, toBoolean, fromFloat, toFloat, FromDouble, toDouble), record);
                break;
            case "entityId2-age":
                Assert.assertEquals(dataType, "Integer", record.toString());
                Assert.assertEquals(toInteger, "18", record.toString());
                assertListNull(Arrays.asList(isDeleted, fromString, toString, fromBoolean, toBoolean, fromInteger,
                        fromBoolean, toBoolean, fromFloat, toFloat, FromDouble, toDouble), record);
                break;
            default:
            }
            rows++;
        }
        Assert.assertEquals(rows, 6);
        return true;
    }

    private void assertListNull(List<String> values, GenericRecord record) {
        for (String val : values) {
            Assert.assertNull(val, record.toString());
        }
    }

    private Boolean verifyChangeList(HdfsDataUnit tgt) {
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            Assert.assertEquals(record.getSchema().getFields().size(), 16, record.toString());
            String rowId = record.get("RowId") != null ? record.get("RowId").toString() : "#";
            String columnId = record.get("ColumnId") != null ? record.get("ColumnId").toString() : "#";
            String dataType = record.get("DataType") != null ? record.get("DataType").toString() : null;
            String isDeleted = record.get("Deleted") == null ? null : record.get("Deleted").toString();
            String fromString = record.get("FromString") == null ? null : record.get("FromString").toString();
            String toString = record.get("ToString") == null ? null : record.get("ToString").toString();
            String fromBoolean = record.get("FromBoolean") == null ? null : record.get("FromBoolean").toString();
            String toBoolean = record.get("ToBoolean") == null ? null : record.get("ToBoolean").toString();
            String fromInteger = record.get("FromInteger") == null ? null : record.get("FromInteger").toString();
            String toInteger = record.get("ToInteger") == null ? null : record.get("ToInteger").toString();
            String fromFloat = record.get("FromFloat") == null ? null : record.get("FromFloat").toString();
            String toFloat = record.get("ToFloat") == null ? null : record.get("ToFloat").toString();
            String FromDouble = record.get("FromDouble") == null ? null : record.get("FromDouble").toString();
            String toDouble = record.get("ToDouble") == null ? null : record.get("ToDouble").toString();
            String key = rowId + "-" + columnId;
            log.info("Key=" + key + " Record=" + record.toString());
            switch (key) {
            case "entityId1-first":
                Assert.assertEquals(dataType, "String", record.toString());
                Assert.assertEquals(fromString, "bill", record.toString());
                Assert.assertEquals(toString, "john", record.toString());
                assertListNull(Arrays.asList(isDeleted, fromBoolean, toBoolean, fromInteger, toInteger, fromBoolean,
                        toBoolean, fromFloat, toFloat, FromDouble, toDouble), record);
                break;
            case "entityId1-last":
                Assert.assertEquals(dataType, "String", record.toString());
                Assert.assertEquals(toString, "smith", record.toString());
                assertListNull(Arrays.asList(isDeleted, fromString, fromBoolean, toBoolean, fromInteger, toInteger,
                        fromBoolean, toBoolean, fromFloat, toFloat, FromDouble, toDouble), record);
                break;
            case "entityId1-salary":
                Assert.assertEquals(dataType, "Integer", record.toString());
                Assert.assertEquals(fromInteger, "2000", record.toString());
                Assert.assertEquals(isDeleted, "true", record.toString());
                assertListNull(Arrays.asList(fromString, toString, fromBoolean, toBoolean, toInteger, fromBoolean,
                        toBoolean, fromFloat, toFloat, FromDouble, toDouble), record);
                break;
            case "entityId2-last":
                Assert.assertEquals(dataType, "String", record.toString());
                Assert.assertEquals(toString, "ann", record.toString());
                assertListNull(Arrays.asList(isDeleted, fromString, fromBoolean, toBoolean, fromInteger, toInteger,
                        fromBoolean, toBoolean, fromFloat, toFloat, FromDouble, toDouble), record);
                break;
            case "entityId2-age":
                Assert.assertEquals(dataType, "Integer", record.toString());
                Assert.assertEquals(toInteger, "18", record.toString());
                assertListNull(Arrays.asList(isDeleted, fromString, toString, fromBoolean, toBoolean, fromInteger,
                        fromBoolean, toBoolean, fromFloat, toFloat, FromDouble, toDouble), record);
                break;
            case "entityId2-#":
                assertListNull(Arrays.asList(dataType, isDeleted, fromString, toString, fromBoolean, toBoolean,
                        fromInteger, toInteger, fromBoolean, toBoolean, fromFloat, toFloat, FromDouble, toDouble),
                        record);
                break;
            case "entityId3-#":
                Assert.assertEquals(isDeleted, "true", record.toString());
                assertListNull(Arrays.asList(dataType, fromString, toString, fromBoolean, toBoolean, fromInteger,
                        toInteger, fromBoolean, toBoolean, fromFloat, toFloat, FromDouble, toDouble), record);
                break;
            case "entityId3-first":
                Assert.assertEquals(fromString, "mary2", record.toString());
                Assert.assertEquals(isDeleted, "true", record.toString());
                Assert.assertEquals(dataType, "String", record.toString());
                assertListNull(Arrays.asList(toString, fromBoolean, toBoolean, fromInteger, toInteger, fromBoolean,
                        toBoolean, fromFloat, toFloat, FromDouble, toDouble), record);
                break;
            case "entityId3-last":
                Assert.assertEquals(fromString, "ann2", record.toString());
                Assert.assertEquals(isDeleted, "true", record.toString());
                Assert.assertEquals(dataType, "String", record.toString());
                assertListNull(Arrays.asList(toString, fromBoolean, toBoolean, fromInteger, toInteger, fromBoolean,
                        toBoolean, fromFloat, toFloat, FromDouble, toDouble), record);
                break;
            case "#-salary":
                Assert.assertEquals(isDeleted, "true", record.toString());
                assertListNull(Arrays.asList(dataType, fromString, toString, fromBoolean, toBoolean, fromInteger,
                        toInteger, fromBoolean, toBoolean, fromFloat, toFloat, FromDouble, toDouble), record);
                break;
            default:
                Assert.fail("Not found! key=" + key);
            }

            rows++;
        }
        Assert.assertEquals(rows, 10);
        return true;
    }

    private Boolean verifyNoChange(HdfsDataUnit tgt) {
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            log.info("Record=" + record.toString());
            Assert.assertEquals(record.getSchema().getFields().size(), 16, record.toString());
            rows++;
        }
        Assert.assertEquals(rows, 0);
        return true;
    }

}
