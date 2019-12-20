package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.spark.CreateCdlEventTableJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CreateCdlEventTableJobTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {

        ExecutorService workers = ThreadPoolUtils.getFixedSizeThreadPool("Create-Cdl-Event-Table-test", 2);
        List<Runnable> runnables = new ArrayList<>();

        Runnable runnable1 = () -> testWithInputTableAccountTable();
        runnables.add(runnable1);

        Runnable runnable2 = () -> testWithInputTableAccountTableApsTable();
        runnables.add(runnable2);

        ThreadPoolUtils.runRunnablesInParallel(workers, runnables, 60, 1);
        workers.shutdownNow();
    }

    private void testWithInputTableAccountTable() {
        List<String> input = uploadDataForInputTableAccountTable();
        CreateCdlEventTableJobConfig config = getConfig();
        SparkJobResult result = runSparkJob(CreateCdlEventTableJob.class, config, input,
                String.format("/tmp/%s/%s/inputTableAccountTable", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyInputTableAccountTable));
    }

    private void testWithInputTableAccountTableApsTable() {
        List<String> input = uploadDataForInputTableAccountTableApsTable();
        CreateCdlEventTableJobConfig config = getConfig();
        SparkJobResult result = runSparkJob(CreateCdlEventTableJob.class, config, input,
                String.format("/tmp/%s/%s/inputTableAccountTableApsTable", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyInputTableAccountTableApsTable));
    }

    private List<String> uploadDataForInputTableAccountTable() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.PeriodId.name(), String.class), //
                Pair.of(InterfaceName.Train.name(), String.class), //
                Pair.of(InterfaceName.Event.name(), String.class) //
        );
        Object[][] data = getInputData();
        input.add(uploadHdfsDataUnit(data, fields));

        fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of("AccountName", String.class) //
        );
        data = getAccountData();
        input.add(uploadHdfsDataUnit(data, fields));

        return input;
    }

    private List<String> uploadDataForInputTableAccountTableApsTable() {
        List<String> input = uploadDataForInputTableAccountTable();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of(InterfaceName.LEAccount_ID.name(), String.class), //
                Pair.of(InterfaceName.Period_ID.name(), String.class), //
                Pair.of("ProductName", String.class) //
        );
        Object[][] data = getApsData();
        input.add(uploadHdfsDataUnit(data, fields));

        return input;
    }

    private Object[][] getInputData() {
        Object[][] data = new Object[][] { //
                { 1, "account1", "period1", "1", "1" }, //
                { 2, "account2", "period2", "1", "0" } //
        };
        return data;
    }

    private Object[][] getAccountData() {
        Object[][] data = new Object[][] { //
                { 1, "account1", "accountName1" }, //
                { 2, "account2", "accountName2" } //
        };
        return data;
    }

    private Object[][] getApsData() {
        Object[][] data = new Object[][] { //
                { 1, "account1", "period1", "product1" }, //
                { 2, "account2", "period2", "product2" } //
        };
        return data;
    }

    private CreateCdlEventTableJobConfig getConfig() {
        CreateCdlEventTableJobConfig config = new CreateCdlEventTableJobConfig();
        config.eventColumn = InterfaceName.Event.name();
        return config;
    }

    private Boolean verifyInputTableAccountTable(HdfsDataUnit tgt) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            Assert.assertEquals(record.getSchema().getFields().size(), 5, record.toString());
            int id = (int) record.get("Id");
            String acocuntId = record.get(InterfaceName.AccountId.name()) == null ? null
                    : record.get(InterfaceName.AccountId.name()).toString();
            String train = record.get(InterfaceName.Train.name()) == null ? null
                    : record.get(InterfaceName.Train.name()).toString();
            String account = record.get("AccountName") == null ? null : record.get("AccountName").toString();
            String event = record.get(InterfaceName.Event.name()) == null ? null
                    : record.get(InterfaceName.Event.name()).toString();
            switch (id) {
            case 1:
                Assert.assertEquals(acocuntId, "account1", record.toString());
                Assert.assertEquals(train, "1", record.toString());
                Assert.assertEquals(account, "accountName1", record.toString());
                Assert.assertEquals(event, "1", record.toString());
                break;
            case 2:
                Assert.assertEquals(acocuntId, "account2", record.toString());
                Assert.assertEquals(train, "1", record.toString());
                Assert.assertEquals(account, "accountName2", record.toString());
                Assert.assertEquals(event, "0", record.toString());
                break;
            default:
                Assert.fail("Should not see a record with id " + id + ": " + record.toString());
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 2L);
        return true;
    }

    private Boolean verifyInputTableAccountTableApsTable(HdfsDataUnit tgt) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            Assert.assertEquals(record.getSchema().getFields().size(), 8, record.toString());
            int id = (int) record.get("Id");
            String acocuntId = record.get(InterfaceName.LEAccount_ID.name()) == null ? null
                    : record.get(InterfaceName.LEAccount_ID.name()).toString();
            String period = record.get(InterfaceName.Period_ID.name()) == null ? null
                    : record.get(InterfaceName.Period_ID.name()).toString();
            String train = record.get(InterfaceName.Train.name()) == null ? null
                    : record.get(InterfaceName.Train.name()).toString();
            String account = record.get("AccountName") == null ? null : record.get("AccountName").toString();
            String event = record.get("Event") == null ? null : record.get("Event").toString();
            String product = record.get("ProductName") == null ? null : record.get("ProductName").toString();
            switch (id) {
            case 1:
                Assert.assertEquals(acocuntId, "account1", record.toString());
                Assert.assertEquals(train, "1", record.toString());
                Assert.assertEquals(account, "accountName1", record.toString());
                Assert.assertEquals(period, "period1", record.toString());
                Assert.assertEquals(event, "1", record.toString());
                Assert.assertEquals(product, "product1", record.toString());
                break;
            case 2:
                Assert.assertEquals(acocuntId, "account2", record.toString());
                Assert.assertEquals(train, "1", record.toString());
                Assert.assertEquals(account, "accountName2", record.toString());
                Assert.assertEquals(period, "period2", record.toString());
                Assert.assertEquals(event, "0", record.toString());
                Assert.assertEquals(product, "product2", record.toString());
                break;
            default:
                Assert.fail("Should not see a record with id " + id + ": " + record.toString());
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 2L);
        return true;
    }

}
