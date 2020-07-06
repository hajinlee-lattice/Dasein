package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeLatticeAccountConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MergeLatticeAccountTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        uploadTestData();
        MergeLatticeAccountConfig config = new MergeLatticeAccountConfig();
        SparkJobResult result = runSparkJob(MergeLatticeAccount.class, config);
        verifyResult(result);
    }

    private void uploadTestData() {
        List<Pair<String, Class<?>>> fields1 = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.LatticeAccountId.name(), String.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr2", String.class), //
                Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class), //
                Pair.of(InterfaceName.CDLUpdatedTime.name(), Long.class));

        Object[][] upload1 = new Object[][] {
                { "Account1", "Lattice1", "1_1", "2_1", 2L, 2L },
                { "Account2", "Lattice2", "1_2", "2_2", 2L, 2L },
                { "Account3", "Lattice3", "1_3", "2_3", 2L, 2L },
                { "Account4", "Lattice4", "1_4", "2_4", 2L, 2L },
                { "Account5", "Lattice5", "1_5", "2_5", 2L, 2L },
                { "Account6", "Lattice6", "1_6", "2_6", 2L, 2L },
        };
        uploadHdfsDataUnit(upload1, fields1);

        List<Pair<String, Class<?>>> fields2 = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.LatticeAccountId.name(), String.class), //
                Pair.of("Attr1", String.class), //
                Pair.of("Attr3", String.class), //
                Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class), //
                Pair.of(InterfaceName.CDLUpdatedTime.name(), Long.class));

        Object[][] upload2 = new Object[][] {
                { "Account1", "Lattice1", "1_1", "3_1", 1L, 1L },
                { "Account2", "Lattice2", "1_2", "3_2", 1L, 1L },
                { "Account3", "Lattice3", "1_3", "3_3", 1L, 1L },
                { "Account7", "Lattice7", "1_4", "3_4", 1L, 1L },
                { "Account8", "Lattice8", "1_5", "3_5", 1L, 1L },
                { "Account9", "Lattice9", "1_6", "3_6", 1L, 1L },
        };
        uploadHdfsDataUnit(upload2, fields2);
    }

    @Override
    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        AtomicInteger nRows = new AtomicInteger(0);
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            // System.out.println(record);
            Assert.assertEquals(record.getSchema().getFields().size(), 6);
            nRows.incrementAndGet();
        });
        Assert.assertEquals(nRows.get(), 9);
        return true;
    }

}
