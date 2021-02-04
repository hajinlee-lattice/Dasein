package com.latticeengines.spark.exposed.job.common;

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
import com.latticeengines.domain.exposed.spark.common.GenerateChangeTableConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GenerateChangeTableTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(GenerateChangeTableTestNG.class);

    private volatile String input1;
    private volatile String input2;
    private volatile String input3;

    @Test(groups = "functional")
    public void testUpsert() {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(this::testCreateChangeTable);
        runnables.add(this::testNoChange);
        runnables.add(this::testCommonColumns);
        ThreadPoolUtils.runInParallel(this.getClass().getSimpleName(), runnables);
    }

    private void testCreateChangeTable() {
        List<String> input = new ArrayList<>();
        input.addAll(upload1Data());
        input.addAll(upload2Data());
        GenerateChangeTableConfig config =  new GenerateChangeTableConfig();
        config.setJoinKey("EntityId");
        config.setAttrsForbidToSet(Collections.singleton("Id"));
        config.setExclusionColumns(Collections.singletonList(InterfaceName.CDLCreatedTime.name()));

        SparkJobResult result = runSparkJob(GenerateChangeTableJob.class, config, input,
                String.format("/tmp/%s/%s/changeTable", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(dataUnit -> verifyChangeTable(dataUnit)));
    }


    private void testNoChange() {
        List<String> input = new ArrayList<>();
        input.addAll(upload1Data());
        input.addAll(upload1Data());
        GenerateChangeTableConfig config =  new GenerateChangeTableConfig();
        config.setJoinKey("EntityId");
        config.setAttrsForbidToSet(Collections.singleton("Id"));
        config.setExclusionColumns(Collections.singletonList(InterfaceName.CDLCreatedTime.name()));

        SparkJobResult result = runSparkJob(GenerateChangeTableJob.class, config, input,
                String.format("/tmp/%s/%s/noChange", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(dataUnit -> verifyNoChange(dataUnit)));
    }


    private void testCommonColumns() {
        List<String> input = new ArrayList<>();
        input.addAll(upload1Data());
        input.addAll(upload3Data());
        GenerateChangeTableConfig config = new GenerateChangeTableConfig();
        config.setJoinKey("EntityId");
        config.setAttrsForbidToSet(Collections.singleton("Id"));
        config.setExclusionColumns(Collections.singletonList(InterfaceName.CDLCreatedTime.name()));

        SparkJobResult result = runSparkJob(GenerateChangeTableJob.class, config, input,
                String.format("/tmp/%s/%s/commonColumns", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(dataUnit -> verifyCommonColumns(dataUnit)));
    }

    // fromTable: old data
    private List<String> upload2Data() {
        List<String> input = new ArrayList<>();
        if (input2 == null) {
            synchronized (this) {
                if (input2 == null) {
                    List<Pair<String, Class<?>>> fields = Arrays.asList( //
                            Pair.of("Id", Integer.class), //
                            Pair.of("EntityId", String.class), //
                            Pair.of("first", String.class), //
                            Pair.of("last", String.class), //
                            Pair.of("salary", Integer.class), //
                            Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class) //

                    );
                    Object[][] data = getInput2Data();
                    log.info("Uploading input2");
                    input2 = uploadHdfsDataUnit(data, fields);
                }
            }
        }
        input.add(input2);
        return input;
    }



    private List<String> upload1Data() {
        List<String> input = new ArrayList<>();
        if (input1 == null) {
            synchronized (this) {
                if (input1 == null) {
                    List<Pair<String, Class<?>>> fields = Arrays.asList( //
                            Pair.of("Id", Integer.class), //
                            Pair.of("EntityId", String.class), //
                            Pair.of("first", String.class), //
                            Pair.of("last", String.class), //
                            Pair.of("age", Integer.class), //
                            Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class) //

                    );
                    Object[][] data = getInput1Data();
                    log.info("Uploading input1");
                    input1 = uploadHdfsDataUnit(data, fields);
                }
            }
        }
        input.add(input1);
        return input;
    }


    private List<String> upload3Data() {
        List<String> input = new ArrayList<>();
        if (input3 == null) {
            synchronized (this) {
                if (input3 == null) {
                    List<Pair<String, Class<?>>> fields = Arrays.asList( //
                            Pair.of("Id", Integer.class), //
                            Pair.of("EntityId", String.class), //
                            Pair.of("first", String.class), //
                            Pair.of("last", String.class), //
                            Pair.of("age", Integer.class), //
                            Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class) //

                    );
                    Object[][] data = getInput3Data();
                    log.info("Uploading input3");
                    input3 = uploadHdfsDataUnit(data, fields);
                }
            }
        }
        input.add(input3);
        return input;
    }

    // old data
    private Object[][] getInput1Data() {
        return new Object[][] { //
                { 1, "entityId1", "john", "smith", null, 1000010L }, //
                { 2, "entityId2", null, "ann", 18, 1000002L } //
        };
    }

    // new data
    private Object[][] getInput2Data() {
        return new Object[][] { //
                { 1, "entityId1", "bill", null, 2000, 1000001L }, //
                { 3, "entityId3", "mary2", "ann2", null, 1000003L } //
        };
    }

    private Object[][] getInput3Data() {
        return new Object[][] { //
                { 1, "entityId1", "john", "smith", null, 1000001L }, //
                { 2, "entityId2", null, "ann", 30, 1000002L } //
        };
    }


    private Boolean verifyChangeTable(HdfsDataUnit tgt) {
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;

        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            Assert.assertEquals(record.getSchema().getFields().size(), 6, record.toString());
            String id = record.get("Id") != null ? record.get("Id").toString() : null;
            String entityId = record.get("EntityId") != null ? record.get("EntityId").toString() : null;
            String first = record.get("first") != null ? record.get("first").toString() : null;
            String last = record.get("last") != null ? record.get("last").toString() : null;
            Integer salary = record.get("salary") != null ? Integer.valueOf(record.get("salary").toString()) : null;
            Long time = record.get(InterfaceName.CDLCreatedTime.name()) != null ?
                    Long.valueOf(record.get(InterfaceName.CDLCreatedTime.name()).toString()) : null;

            switch(entityId) {
                case "entityId1":
                    Assert.assertEquals(id, "1");
                    Assert.assertEquals(first, "bill");
                    Assert.assertNull(last);
                    Assert.assertEquals(salary.intValue(), 2000);
                    Assert.assertEquals(time.longValue(),  1000001L);
                    break;
                case "entityId2":
                    Assert.assertEquals(id, "2");
                    Assert.assertNull(first);
                    Assert.assertNull(last);
                    Assert.assertNull(salary);
                    Assert.assertNull(time);
                    break;
                case "entityId3":
                    Assert.assertEquals(id, "3");
                    Assert.assertEquals(first, "mary2");
                    Assert.assertEquals(last, "ann2");
                    Assert.assertNull(salary);
                    Assert.assertEquals(time.longValue(),  1000003L);
                    break;
                    default:
                    break;

            }
            rows++;
        }
        Assert.assertEquals(rows, 3);
        return true;
    }

    private Boolean verifyNoChange(HdfsDataUnit tgt) {
        Assert.assertEquals(tgt.getCount().longValue(), 0L);
        return true;
    }


    private Boolean verifyCommonColumns(HdfsDataUnit tgt) {
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;

        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            Assert.assertEquals(record.getSchema().getFields().size(), 6, record.toString());
            String id = record.get("Id") != null ? record.get("Id").toString() : null;
            String entityId = record.get("EntityId") != null ? record.get("EntityId").toString() : null;
            String first = record.get("first") != null ? record.get("first").toString() : null;
            String last = record.get("last") != null ? record.get("last").toString() : null;
            Integer age = record.get("age") != null ? Integer.valueOf(record.get("age").toString()) : null;
            Long time = record.get(InterfaceName.CDLCreatedTime.name()) != null ?
                    Long.valueOf(record.get(InterfaceName.CDLCreatedTime.name()).toString()) : null;

            switch (entityId) {
                case "entityId2":
                    Assert.assertEquals(id, "2");
                    Assert.assertNull(first);
                    Assert.assertEquals(last, "ann");
                    Assert.assertEquals(age.intValue(), 30);
                    Assert.assertEquals(time.longValue(), 1000002L);
                    break;
                default:
                    break;
            }
            rows++;
        }

        Assert.assertEquals(rows, 1);
        return true;
    }
}
