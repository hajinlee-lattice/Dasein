package com.latticeengines.spark.exposed.job.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.FilterChangelistConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class FilterChangelistJobTestNG extends SparkJobFunctionalTestNGBase {

    List<String> input = new LinkedList<>();

    @Test(groups = "functional")
    public void test() {
        uploadData();
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(this::testFilter);
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
                Pair.of("FromBoolean", Boolean.class), //
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
                { "168882gwjopqxyz", "CompanyName", "String", null, "Google", "Alphabet", null, null, null, null, null, null,
                        null, null, null, null }, //
                { "0024yb6gnhyg2iqw", "City", "String", null, "MountainView", "San Jose", null, null, null, null, null, null,
                        null, null, null, null }, //
                { "02slxn2gwjjabcdef", "CompanyName", "String", null, "Lattice", "D&B Lattice", null, null, null, null, null,
                        null, null, null, null, null }, //
                { "02slxn2gwjjm1qxs", "CompanyName", "String", true, "Facebook", null, null, null, null, null, null, null,
                        null, null, null, null }, //
                { "1048abefwwss3jho", "Country", "String", null, "USA", "France", null, null, null, null, null, null, null,
                        null, null, null } };

        input.add(uploadHdfsDataUnit(changeListData, changeList));
    }

    private void testFilter() {
        FilterChangelistConfig config = new FilterChangelistConfig();
        config.setKey(InterfaceName.AccountId.name());
        config.setColumnId("CompanyName");
        config.setSelectColumns(Arrays.asList(InterfaceName.AccountId.name(), "CompanyName"));

        SparkJobResult result = runSparkJob(FilterChangelistJob.class, config, input,
                String.format("/tmp/%s/%s/testFilter", leStack, this.getClass().getSimpleName()));
        verifyResult(result);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verifyChanged, this::verifyDeleted);
    }

    private Boolean verifyChanged(HdfsDataUnit tgt) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            System.out.println("accountId: " + accountId);
            switch (accountId) {
            case "168882gwjopqxyz":
                Assert.assertEquals(record.get("CompanyName").toString(), "Alphabet");
                break;
            case "02slxn2gwjjabcdef":
                Assert.assertEquals(record.get("CompanyName").toString(), "D&B Lattice");
                break;
            default:
                Assert.fail("Should not see a record with accountId : " + accountId);
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 2L);
        return true;
    }

    private Boolean verifyDeleted(HdfsDataUnit tgt) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            String rowId = record.get("RowId").toString();
            switch (rowId) {
            case "02slxn2gwjjm1qxs":
                Assert.assertEquals(record.get("Deleted"), true);
                Assert.assertEquals(record.get("FromString").toString(), "Facebook");
                break;
            default:
                Assert.fail("Should not see a record with rowId : " + rowId);
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 1L);
        return true;
    }
}
