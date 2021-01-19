package com.latticeengines.spark.exposed.job.common;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.UpsertConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class UpsertWithEraseByNullTestNG extends SparkJobFunctionalTestNGBase {

    private ThreadLocal<UpsertResultVerifier> localVerifier;

    private String templateColumn = "__template__";

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        uploadData();
    }

    @Test(groups = "functional", dataProvider = "upsertWithEraseTests")
    public void testUpsertWithErase(UpsertConfig config, UpsertResultVerifier verifier) {
        SparkJobResult result = runSparkJob(UpsertJob.class, config);
        localVerifier = new ThreadLocal<>();
        localVerifier.set(verifier);
        verifyResult(result);
    }

    private void uploadData() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Task_a__Attr1", String.class), //
                Pair.of("Task_a__Attr2", Long.class), //
                Pair.of("Task_b__Attr1", String.class) //
        );
        Object[][] data = new Object[][] { //
                { 1, "1", 1L, "1b" }, //
                { 2, "2", 2L, "2b" }, //
                { 3, "3", 3L, "3b" }, //
                { 4, null, 4L, "4b" }, //
        };
        uploadHdfsDataUnit(data, fields);

        fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", Long.class), //
                Pair.of("Attr3", Boolean.class), //
                Pair.of("Erase_Id", Boolean.class), //
                Pair.of("Erase_Attr1", Boolean.class), //
                Pair.of("Erase_Attr2", Boolean.class), //
                Pair.of("Erase_Attr3", Boolean.class), //
                Pair.of(templateColumn, String.class) //
        );
        data = new Object[][] { //
                { 2, "22", 22L, true, null, null, null, null, "Task_a" }, //
                { 3, null, null, false, null, true, true, null, "Task_a" }, //
                { 4, "24", null, null, null, null, null, null, "Task_a" }, //
                { 5, "25", null, false, null, null, true, null, "Task_a" } //
        };
        uploadHdfsDataUnit(data, fields);
    }

    @DataProvider(name = "upsertWithEraseTests")
    public Object[][] provideUpsertWithEraseTestData() {
        return new Object[][] { //
                { getEraseByNullConfig(), getEraseByNullVerifier() } };
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        AtomicInteger count = new AtomicInteger();
        final UpsertResultVerifier verifier = localVerifier.get();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            count.incrementAndGet();
            verifier.verifyRecord(record);
        });
        verifier.verifyCount(count.get());
        return true;
    }

    private UpsertResultVerifier getEraseByNullVerifier() {
        return new UpsertResultVerifier() {
            @Override
            public void verifyRecord(GenericRecord record) {
                Assert.assertEquals(record.getSchema().getFields().size(), 6);
                int id = (int) record.get("Id");
                String attr1 = record.get("Task_a__Attr1") == null ? null : record.get("Task_a__Attr1").toString();
                Long attr2 = record.get("Task_a__Attr2") == null ? null : (long) record.get("Task_a__Attr2");
                Boolean attr3 = record.get("Task_a__Attr3") == null ? null : (boolean) record.get("Task_a__Attr3");
                String bAttr1 = record.get("Task_b__Attr1") == null ? null : record.get("Task_b__Attr1").toString();
                switch (id) {
                case 1:
                    Assert.assertEquals(attr1, "1", record.toString());
                    Assert.assertEquals(attr2, Long.valueOf(1), record.toString());
                    Assert.assertNull(attr3, record.toString());
                    Assert.assertEquals(bAttr1, "1b", record.toString());
                    break;
                case 2:
                    Assert.assertEquals(attr1, "22", record.toString());
                    Assert.assertEquals(attr2, Long.valueOf(22), record.toString());
                    Assert.assertEquals(attr3, Boolean.TRUE, record.toString());
                    Assert.assertEquals(bAttr1, "2b", record.toString());
                    break;
                case 3:
                    Assert.assertNull(attr1, record.toString());
                    Assert.assertNull(attr2, record.toString());
                    Assert.assertEquals(attr3, Boolean.FALSE, record.toString());
                    Assert.assertEquals(bAttr1, "3b", record.toString());
                    break;
                case 4:
                    Assert.assertEquals(attr1, "24", record.toString());
                    Assert.assertEquals(attr2, Long.valueOf(4), record.toString());
                    Assert.assertNull(attr3, record.toString());
                    Assert.assertEquals(bAttr1, "4b", record.toString());
                    break;
                case 5:
                    Assert.assertEquals(attr1, "25", record.toString());
                    Assert.assertNull(attr2, record.toString());
                    Assert.assertEquals(attr3, Boolean.FALSE, record.toString());
                    Assert.assertNull(bAttr1, record.toString());
                    break;
                default:
                    Assert.fail("Should not see a record with id " + id + ": " + record.toString());
                }
            }

            @Override
            public void verifyCount(int count) {

            }
        };
    }

    private UpsertConfig getEraseByNullConfig() {
        UpsertConfig config = UpsertConfig.joinBy("Id");
        config.setNotOverwriteByNull(true);
        config.setEraseByNullEnabled(true);
        config.setAddInputSystemBatch(true);
        return config;
    }

    private interface UpsertResultVerifier {
        void verifyRecord(GenericRecord record);

        void verifyCount(int count);
    }

}
