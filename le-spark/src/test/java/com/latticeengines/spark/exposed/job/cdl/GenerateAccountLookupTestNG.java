package com.latticeengines.spark.exposed.job.cdl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateAccountLookupConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GenerateAccountLookupTestNG extends SparkJobFunctionalTestNGBase {

    private static final String LOOKUP_ID_1 = "sfdc_id_1";
    private static final String LOOKUP_ID_2 = "sfdc_id_2";
    private static final String CUSTOMER_ACCOUNT_ID = InterfaceName.CustomerAccountId.name();

    @Test(groups = "functional")
    public void testNoCustomerAccountId() {
        String input = uploadData();
        GenerateAccountLookupConfig config = new GenerateAccountLookupConfig();
        config.setLookupIds(Arrays.asList(LOOKUP_ID_1, LOOKUP_ID_2));
        SparkJobResult result = runSparkJob(GenerateAccountLookupJob.class, config, Collections.singletonList(input), getWorkspace());
        verify(result, Collections.singletonList(this::verifyNoCustomerAccountId));
    }

    @Test(groups = "functional")
    public void testWithCustomerAccountId() {
        String input = uploadDataWithCustomerAccountId();
        GenerateAccountLookupConfig config = new GenerateAccountLookupConfig();
        config.setLookupIds(Arrays.asList(LOOKUP_ID_1, LOOKUP_ID_2));
        SparkJobResult result = runSparkJob(GenerateAccountLookupJob.class, config, Collections.singletonList(input), getWorkspace());
        verify(result, Collections.singletonList(this::verifyHasCustomerAccountId));
    }

    private String uploadData() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(LOOKUP_ID_1, String.class), //
                Pair.of(LOOKUP_ID_2, String.class) //
        );
        Object[][] data = new Object[][] { //
                { "1", "s1_1", "s2_1" },
                { "2", "s1_2", null },
                { "3", null, "s2_3" }, //
                { null, null, null }, // FIXME remove this tmp test case after null accountId issue is fixed
        };
        return uploadHdfsDataUnit(data, fields);
    }

    private String uploadDataWithCustomerAccountId() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(LOOKUP_ID_1, String.class), //
                Pair.of(LOOKUP_ID_2, String.class), //
                Pair.of(CUSTOMER_ACCOUNT_ID, String.class)
        );
        Object[][] data = new Object[][] { //
                { "1", "s1_1", "s2_1", "ca1" }, //
                { "2", "s1_2", null, "ca2" }, //
                { "3", null, "s2_3", null }, //
                { null, null, null, null }, // FIXME remove this tmp test case after null accountId issue is fixed
        };
        return uploadHdfsDataUnit(data, fields);
    }

    protected Boolean verifyNoCustomerAccountId(HdfsDataUnit tgt) {
        AtomicInteger count = new AtomicInteger();
        Set<String> expectedKeys = new HashSet<>(Arrays.asList( //
                "AccountId_1", "AccountId_2", "AccountId_3", //
                "sfdc_id_1_s1_1", "sfdc_id_1_s1_2", //
                "sfdc_id_2_s2_1", "sfdc_id_2_s2_3" //
        ));
        int expectedCount = expectedKeys.size();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            count.incrementAndGet();
            expectedKeys.remove(record.get(InterfaceName.AtlasLookupKey.name()).toString());
        });
        Assert.assertEquals(count.get(), expectedCount);
        Assert.assertTrue(expectedKeys.isEmpty());
        return true;
    }

    protected Boolean verifyHasCustomerAccountId(HdfsDataUnit tgt) {
        AtomicInteger count = new AtomicInteger();
        Set<String> expectedKeys = new HashSet<>(Arrays.asList( //
                "AccountId_1", "AccountId_2", "AccountId_3", //
                "sfdc_id_1_s1_1", "sfdc_id_1_s1_2", //
                "sfdc_id_2_s2_1", "sfdc_id_2_s2_3", //
                "CustomerAccountId_ca1", "CustomerAccountId_ca2" //
        ));
        int expectedCount = expectedKeys.size();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            count.incrementAndGet();
            expectedKeys.remove(record.get(InterfaceName.AtlasLookupKey.name()).toString());
        });
        Assert.assertEquals(count.get(), expectedCount);
        Assert.assertTrue(expectedKeys.isEmpty());
        return true;
    }

}
