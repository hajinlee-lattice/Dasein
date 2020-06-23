package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.JoinChangeListAccountBatchConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class JoinChangeListAccountBatchTestNG extends SparkJobFunctionalTestNGBase {

    List<String> input = new LinkedList<>();

    @Test(groups = "functional")
    public void test() {
        uploadData();
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(this::testJoin);
        ThreadPoolUtils.runInParallel(this.getClass().getSimpleName(), runnables);
    }

    private void uploadData() {
        List<Pair<String, Class<?>>> changeList = Arrays.asList( //
                Pair.of("RowId", String.class), //
                Pair.of("ColumnId", String.class), //
                Pair.of("DataType", String.class), //
                Pair.of("Deleted", Boolean.class), //
                Pair.of("FromString", String.class), //
                Pair.of("ToString", String.class), //
                Pair.of("ToBoolean", Boolean.class), //
                Pair.of("FromInteger", Integer.class), //
                Pair.of("ToInteger", Integer.class), //
                Pair.of("FromLong", Long.class), //
                Pair.of("ToLong", Long.class), //
                Pair.of("FromFloat", Float.class), //
                Pair.of("ToFloat", Float.class), //
                Pair.of("FromDouble", Float.class), //
                Pair.of("ToDouble", Float.class) //
        );
        Object[][] changeListData = new Object[][] { //
                { "0024yb6gnhyg2iqw", "City", "String", null, "MountainView", "San Jose", null, null, null, null, null,
                        null, null, null, null }, //
                { "02slxn2gwjjm1qxs", "CompanyName", "String", true, null, null, null, null, null, null, null, null,
                        null, null, null }, //
                { "1048abefwwss3jho", "Country", "String", null, "USA", "France", null, null, null, null, null, null,
                        null, null, null } };

        List<Pair<String, Class<?>>> accountBatch = Arrays.asList( //
                Pair.of("AccountId", String.class), //
                Pair.of("EntityId", String.class), //
                Pair.of("LatticeAccountId", String.class), //
                Pair.of("CompanyName", String.class), //
                Pair.of("Country", String.class), //
                Pair.of("AnnualRevenue", Double.class), //
                Pair.of("City", String.class));
        Object[][] accountBatchData = new Object[][] { //
                { "0024yb6gnhyg2iqw", "0024yb6gnhyg2iqw", "0420002331653", "Google", "USA", 18027911300.0,
                        "MountainView" }, //
                { "02slxn2gwjjm1qxs", "02p9l4uxf1tlm3ti", "0270295252190", "FB", "USA", 50880000000.0, "MountainView" }, //
                { "00es5ifcql5wrex4", "00es5ifcql5wrex4", "0370002180579", "DnB", "USA", 10066824000.0, "New York" } //
        };

        input.add(uploadHdfsDataUnit(changeListData, changeList));
        input.add(uploadHdfsDataUnit(accountBatchData, accountBatch));
    }

    private void testJoin() {
        JoinChangeListAccountBatchConfig config = new JoinChangeListAccountBatchConfig();
        config.setJoinKey(InterfaceName.AccountId.name());
        config.setSelectColumns(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.LatticeAccountId.name()));

        SparkJobResult result = runSparkJob(JoinChangeListAccountBatchJob.class, config, input,
                String.format("/tmp/%s/%s/testJoin", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyResult));
    }

    private Boolean verifyResult(HdfsDataUnit tgt) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            String accountId = record.get("AccountId").toString();
            switch (accountId) {
            case "0024yb6gnhyg2iqw":
                Assert.assertEquals(record.get("LatticeAccountId").toString(), "0420002331653");
                break;
            case "02slxn2gwjjm1qxs":
                Assert.assertEquals(record.get("LatticeAccountId").toString(), "0270295252190");
                break;
            default:
                Assert.fail("Should not see a record with accountId : " + accountId);
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 2L);
        return true;
    }
}
