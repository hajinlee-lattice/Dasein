package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeLatticeAccountConfig;
import com.latticeengines.domain.exposed.spark.common.ChangeListConstants;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MergeLatticeAccountTestNG extends SparkJobFunctionalTestNGBase {

    private Function<HdfsDataUnit, Boolean> targetVerifier;

    @Test(groups = "functional")
    public void test() {
        uploadTestData();

        // horizontal mode
        MergeLatticeAccountConfig config = new MergeLatticeAccountConfig();
        config.setMergeMode(ChangeListConstants.HorizontalMode);
        inputProvider = () -> Arrays.asList(getUploadedDataSet(0), getUploadedDataSet(1));
        SparkJobResult result = runSparkJob(MergeLatticeAccount.class, config);
        targetVerifier = this::verifyHorizontalTarget;
        verifyResult(result);

        // vertical mode
        config = new MergeLatticeAccountConfig();
        config.setMergeMode(ChangeListConstants.VerticalMode);
        inputProvider = () -> Arrays.asList(getUploadedDataSet(0), getUploadedDataSet(2));
        result = runSparkJob(MergeLatticeAccount.class, config);
        targetVerifier = this::verifyVerticalTarget;
        verifyResult(result);
    }

    private void uploadTestData() {
        // old LatticeAccount
        List<Pair<String, Class<?>>> fields1 = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.LatticeAccountId.name(), String.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", String.class), //
                Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class), //
                Pair.of(InterfaceName.CDLUpdatedTime.name(), Long.class));

        Object[][] upload1 = new Object[][] {
                { "Account1", "Lattice1", "1_1", "2_1", 1L, 1L },
                { "Account2", "Lattice2", "1_2", "2_2", 1L, 1L },
                { "Account3", "Lattice3", "1_3", "2_3", 1L, 1L },
                { "Account4", "Lattice4", "1_4", "2_4", 1L, 1L },
                { "Account5", "Lattice5", "1_5", "2_5", 1L, 1L },
                { "Account6", "Lattice6", "1_6", "2_6", 1L, 1L },
        };
        uploadHdfsDataUnit(upload1, fields1);

        // new LatticeAccount - horizontal
        List<Pair<String, Class<?>>> fields2 = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.LatticeAccountId.name(), String.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr3", String.class), //
                Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class), //
                Pair.of(InterfaceName.CDLUpdatedTime.name(), Long.class));

        Object[][] upload2 = new Object[][] {
                { "Account1", "Lattice1", "1_1_a", "3_1", 2L, 2L },
                { "Account2", "Lattice2", "1_2_a", "3_2", 2L, 2L },
                { "Account3", "Lattice3", "1_3_a", "3_3", 2L, 2L },
                { "Account7", "Lattice7", "1_4_a", "3_4", 2L, 2L },
                { "Account8", "Lattice8", "1_5_a", "3_5", 2L, 2L },
                { "Account9", "Lattice9", "1_6_a", "3_6", 2L, 2L },
        };
        uploadHdfsDataUnit(upload2, fields2);

        // new LatticeAccount - vertical
        List<Pair<String, Class<?>>> fields3 = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.LatticeAccountId.name(), String.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr3", String.class));

        Object[][] upload3 = new Object[][] {
                { "Account1", "Lattice1", "1_1_a", "3_1" },
                { "Account2", "Lattice2", "1_2_a", "3_2" },
                { "Account3", "Lattice3", "1_3_a", "3_3" }
        };
        uploadHdfsDataUnit(upload3, fields3);
    }
    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        return targetVerifier.apply(tgt);
    }

    private Boolean verifyHorizontalTarget(HdfsDataUnit tgt) {
        AtomicInteger nRows = new AtomicInteger(0);
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            // System.out.println(record);
            Assert.assertEquals(record.getSchema().getFields().size(), 6);
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            switch (accountId) {
                case "Account4":
                case "Account5":
                case "Account6":
                    Assert.assertFalse(record.get("Attr1").toString().endsWith("_a"));
                    Assert.assertNull(record.get("Attr3"));
                    Assert.assertEquals(record.get("CDLUpdatedTime"), 1L);
                    break;
                default:
                    Assert.assertTrue(record.get("Attr1").toString().endsWith("_a"));
                    Assert.assertNotNull(record.get("Attr3"));
                    Assert.assertEquals(record.get("CDLUpdatedTime"), 2L);
            }
            nRows.incrementAndGet();
        });
        Assert.assertEquals(nRows.get(), 9);
        return true;
    }

    private Boolean verifyVerticalTarget(HdfsDataUnit tgt) {
        AtomicInteger nRows = new AtomicInteger(0);
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            // System.out.println(record);
            Assert.assertEquals(record.getSchema().getFields().size(), 7);
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            switch (accountId) {
                case "Account1":
                case "Account2":
                case "Account3":
                    Assert.assertTrue(record.get("Attr1").toString().endsWith("_a"));
                    Assert.assertNotNull(record.get("Attr3"));
                    Assert.assertNotEquals(record.get("CDLUpdatedTime"), 1L);
                    break;
                default:
                    Assert.assertFalse(record.get("Attr1").toString().endsWith("_a"));
                    Assert.assertNull(record.get("Attr3"));
                    Assert.assertEquals(record.get("CDLUpdatedTime"), 1L);
            }
            Assert.assertEquals(record.get("CDLCreatedTime"), 1L);
            nRows.incrementAndGet();
        });
        Assert.assertEquals(nRows.get(), 6);
        return true;
    }

}
