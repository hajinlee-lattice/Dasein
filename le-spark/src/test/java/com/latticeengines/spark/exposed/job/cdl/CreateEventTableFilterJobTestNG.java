package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CreateEventTableFilterJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CreateEventTableFilterJobTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        List<Runnable> runnables = Arrays.asList( //
                this::testWithoutRevenue, //
                this::testWithRevenue //
        );
        ThreadPoolUtils.runInParallel(this.getClass().getSimpleName(), runnables);
    }

    private void testWithoutRevenue() {
        List<String> input = uploadDataWithoutRevenue();
        CreateEventTableFilterJobConfig config = getConfig();
        SparkJobResult result = runSparkJob(CreateEventTableFilterJob.class, config, input,
                String.format("/tmp/%s/%s/withoutRevenue", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyWithoutRevenue));
    }

    private void testWithRevenue() {
        List<String> input = uploadDataWithRevenue();
        CreateEventTableFilterJobConfig config = getConfig();
        SparkJobResult result = runSparkJob(CreateEventTableFilterJob.class, config, input,
                String.format("/tmp/%s/%s/withRevenue", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyWithRevenue));
    }

    private List<String> uploadDataWithoutRevenue() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = getTrainFields();
        Object[][] data = getTrainData();
        input.add(uploadHdfsDataUnit(data, fields));

        fields = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.PeriodId.name(), String.class) //
        );
        data = getEventDataWithoutRevenue();
        input.add(uploadHdfsDataUnit(data, fields));

        return input;
    }

    private List<Pair<String, Class<?>>> getTrainFields() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.PeriodId.name(), String.class) //
        );
        return fields;
    }

    private List<String> uploadDataWithRevenue() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = getTrainFields();
        Object[][] data = getTrainData();
        input.add(uploadHdfsDataUnit(data, fields));

        fields = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(InterfaceName.PeriodId.name(), String.class), //
                Pair.of(InterfaceName.__Revenue.name(), Double.class) //
        );
        data = getEventDataWithRevenue();
        input.add(uploadHdfsDataUnit(data, fields));

        return input;
    }

    private Object[][] getTrainData() {
        Object[][] data = new Object[][] { //
                { "account1", "period1" }, //
                { "account2", "period2" }, //
                { "account3", "period3" } //
        };
        return data;
    }

    private Object[][] getEventDataWithoutRevenue() {
        Object[][] data = new Object[][] { //
                { "account1", "period1" }, //
                { "account2", "period2" } //
        };
        return data;
    }

    private Object[][] getEventDataWithRevenue() {
        Object[][] data = new Object[][] { //
                { "account1", "period1", 100D }, //
                { "account2", "period2", 200D } //
        };
        return data;
    }

    private CreateEventTableFilterJobConfig getConfig() {
        CreateEventTableFilterJobConfig config = new CreateEventTableFilterJobConfig();
        config.setEventColumn(InterfaceName.Event.name());
        return config;
    }

    private Boolean verifyWithoutRevenue(HdfsDataUnit tgt) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            Assert.assertEquals(record.getSchema().getFields().size(), 4, record.toString());
            String accountId = record.get(InterfaceName.AccountId.name()) == null ? null
                    : record.get(InterfaceName.AccountId.name()).toString();
            String periodId = record.get(InterfaceName.PeriodId.name()) == null ? null
                    : record.get(InterfaceName.PeriodId.name()).toString();
            String train = record.get(InterfaceName.Train.name()) == null ? null
                    : record.get(InterfaceName.Train.name()).toString();
            String event = record.get(InterfaceName.Event.name()) == null ? null
                    : record.get(InterfaceName.Event.name()).toString();
            switch (accountId) {
            case "account1":
                Assert.assertEquals(periodId, "period1", record.toString());
                Assert.assertEquals(train, "1", record.toString());
                Assert.assertEquals(event, "1", record.toString());
                break;
            case "account2":
                Assert.assertEquals(periodId, "period2", record.toString());
                Assert.assertEquals(train, "1", record.toString());
                Assert.assertEquals(event, "1", record.toString());
                break;
            case "account3":
                Assert.assertEquals(periodId, "period3", record.toString());
                Assert.assertEquals(train, "1", record.toString());
                Assert.assertEquals(event, "0", record.toString());
                break;
            default:
                Assert.fail("Should not see a record with accountId= " + accountId + ": " + record.toString());
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 3L);
        return true;
    }

    private Boolean verifyWithRevenue(HdfsDataUnit tgt) {
        final AtomicLong count = new AtomicLong();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            Assert.assertEquals(record.getSchema().getFields().size(), 5, record.toString());
            String accountId = record.get(InterfaceName.AccountId.name()) == null ? null
                    : record.get(InterfaceName.AccountId.name()).toString();
            String periodId = record.get(InterfaceName.PeriodId.name()) == null ? null
                    : record.get(InterfaceName.PeriodId.name()).toString();
            String train = record.get(InterfaceName.Train.name()) == null ? null
                    : record.get(InterfaceName.Train.name()).toString();
            String event = record.get(InterfaceName.Event.name()) == null ? null
                    : record.get(InterfaceName.Event.name()).toString();
            String revenue = record.get(InterfaceName.__Revenue.name()) == null ? null
                    : record.get(InterfaceName.__Revenue.name()).toString();
            switch (accountId) {
            case "account1":
                Assert.assertEquals(periodId, "period1", record.toString());
                Assert.assertEquals(train, "1", record.toString());
                Assert.assertEquals(event, "1", record.toString());
                Assert.assertEquals(revenue, "100.0", record.toString());
                break;
            case "account2":
                Assert.assertEquals(periodId, "period2", record.toString());
                Assert.assertEquals(train, "1", record.toString());
                Assert.assertEquals(event, "1", record.toString());
                Assert.assertEquals(revenue, "200.0", record.toString());
                break;
            case "account3":
                Assert.assertEquals(periodId, "period3", record.toString());
                Assert.assertEquals(train, "1", record.toString());
                Assert.assertEquals(event, "0", record.toString());
                Assert.assertEquals(revenue, "0.0", record.toString());
                break;
            default:
                Assert.fail("Should not see a record with accontId= " + accountId + ": " + record.toString());
            }
            count.addAndGet(1);
        });
        Assert.assertEquals(count.get(), 3L);
        return true;
    }

}
