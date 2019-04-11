package com.latticeengines.spark.exposed.job.common;

import java.util.Arrays;
import java.util.Collections;
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

public class UpsertTestNG extends SparkJobFunctionalTestNGBase {

    private ThreadLocal<UpsertResultVerifier> localVerifier;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
        uploadData();
    }

    @Test(groups = "functional", dataProvider = "upsertTests")
    public void testUpsert(UpsertConfig config, UpsertResultVerifier verifier) {
        SparkJobResult result = runSparkJob(UpsertJob.class, config);
        localVerifier = new ThreadLocal<>();
        localVerifier.set(verifier);
        verifyResult(result);
    }

    private void uploadData() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", Long.class) //
        );
        Object[][] data = new Object[][] { //
                {1, "1", 1L}, //
                {2, "2", null}, //
                {3, null, 3L}, //
                {4, "4", 4L}, //
        };
        uploadHdfsDataUnit(data, fields);

        fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", Long.class), //
                Pair.of("Attr3", Boolean.class) //
        );
        data = new Object[][] { //
                {2, "22", null, true}, //
                {3, "23", -3L, false}, //
                {4, "24", null, null}, //
                {5, "25", -5L, false} //
        };
        uploadHdfsDataUnit(data, fields);
    }

    @DataProvider(name = "upsertTests")
    public Object[][] provideUpsertTestData() {
        return new Object[][] { //
                { getJoinByConfig(),  getJoinByVerifier() }, //
                { getAttr2FromLhsConfig(), getAttr2FromLhsByVerifier() }, //
        };
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

    private UpsertConfig getJoinByConfig() {
        return UpsertConfig.joinBy("Id");
    }

    private UpsertResultVerifier getJoinByVerifier() {
        return new UpsertResultVerifier() {
            @Override
            public void verifyRecord(GenericRecord record) {
                int id = (int) record.get("Id");
                String attr1 = record.get("Attr1") == null ? null : record.get("Attr1").toString();
                Long attr2 = record.get("Attr2") == null ? null : (long) record.get("Attr2");
                Boolean attr3 = record.get("Attr3") == null ? null : (boolean) record.get("Attr3");
                switch (id) {
                    case 1:
                        Assert.assertEquals(attr1, "1", record.toString());
                        Assert.assertEquals(attr2, Long.valueOf(1), record.toString());
                        Assert.assertNull(attr3, record.toString());
                        break;
                    case 2:
                        Assert.assertEquals(attr1, "22", record.toString());
                        Assert.assertNull(attr2, record.toString());
                        Assert.assertEquals(attr3, Boolean.TRUE, record.toString());
                        break;
                    case 3:
                        Assert.assertEquals(attr1, "23", record.toString());
                        Assert.assertEquals(attr2, Long.valueOf(-3), record.toString());
                        Assert.assertEquals(attr3, Boolean.FALSE, record.toString());
                        break;
                    case 4:
                        Assert.assertEquals(attr1, "24", record.toString());
                        Assert.assertNull(attr2, record.toString());
                        Assert.assertNull(attr3, record.toString());
                        break;
                    case 5:
                        Assert.assertEquals(attr1, "25", record.toString());
                        Assert.assertEquals(attr2, Long.valueOf(-5), record.toString());
                        Assert.assertEquals(attr3, Boolean.FALSE, record.toString());
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

    private UpsertConfig getAttr2FromLhsConfig() {
        UpsertConfig config = UpsertConfig.joinBy("Id");
        config.setColsFromLhs(Collections.singletonList("Attr2"));
        return config;
    }

    private UpsertResultVerifier getAttr2FromLhsByVerifier() {
        return new UpsertResultVerifier() {
            @Override
            public void verifyRecord(GenericRecord record) {
                int id = (int) record.get("Id");
                String attr1 = record.get("Attr1") == null ? null : record.get("Attr1").toString();
                Long attr2 = record.get("Attr2") == null ? null : (long) record.get("Attr2");
                Boolean attr3 = record.get("Attr3") == null ? null : (boolean) record.get("Attr3");
                switch (id) {
                    case 1:
                        Assert.assertEquals(attr1, "1", record.toString());
                        Assert.assertEquals(attr2, Long.valueOf(1), record.toString());
                        Assert.assertNull(attr3, record.toString());
                        break;
                    case 2:
                        Assert.assertEquals(attr1, "22", record.toString());
                        Assert.assertNull(attr2, record.toString());
                        Assert.assertEquals(attr3, Boolean.TRUE, record.toString());
                        break;
                    case 3:
                        Assert.assertEquals(attr1, "23", record.toString());
                        Assert.assertEquals(attr2, Long.valueOf(3), record.toString());
                        Assert.assertEquals(attr3, Boolean.FALSE, record.toString());
                        break;
                    case 4:
                        Assert.assertEquals(attr1, "24", record.toString());
                        Assert.assertEquals(attr2, Long.valueOf(4), record.toString());
                        Assert.assertNull(attr3, record.toString());
                        break;
                    case 5:
                        Assert.assertEquals(attr1, "25", record.toString());
                        Assert.assertEquals(attr2, Long.valueOf(-5), record.toString());
                        Assert.assertEquals(attr3, Boolean.FALSE, record.toString());
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

    private interface UpsertResultVerifier {
        void verifyRecord(GenericRecord record);
        void verifyCount(int count);
    }

}
