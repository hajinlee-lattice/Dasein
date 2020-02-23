package com.latticeengines.spark.exposed.job.cdl;

import java.util.ArrayList;
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
import com.latticeengines.domain.exposed.spark.cdl.SelectByColumnConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class SelectByColumnJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final List<Pair<String, Class<?>>> SOURCE_FIELDS = Collections.singletonList(
            Pair.of(InterfaceName.AccountId.name(), String.class));

    private static final List<Pair<String, Class<?>>> BASE_FIELDS = Arrays.asList(
            Pair.of(InterfaceName.AccountId.name(), String.class),
            Pair.of(InterfaceName.ContactId.name(), String.class),
            Pair.of(InterfaceName.CompanyName.name(), String.class),
            Pair.of(InterfaceName.__Row_Count__.name(), Integer.class));

    private String setupBase() {
        Object[][] data = new Object[][]{ //
                {"acct1", "ct1", "DnB", 1}, //
                {"acct1", "ct2", "DnB", 2}, //
                {"acct2", "ct3", "DnB", 3}, //
                {"acct2", "ct4", "LE", 4}, //
                {"acct3", "ct4", "LE", 5}, //
                {"acct3", "ct2", "LE", 6}, //
                {"acct4", "ct6", "LE", 7}, //
        };
        return uploadHdfsDataUnit(data, BASE_FIELDS);
    }

    private String setupSource() {
        Object[][] data = new Object[][]{
                {"acct1"},
                {"acct3"},
        };
        return uploadHdfsDataUnit(data, SOURCE_FIELDS);
    }

    @Test(groups = "functional")
    public void testSelectByColumn() {
        List<String> inputs = new ArrayList<>();
        inputs.add(setupSource());
        inputs.add(setupBase());
        SelectByColumnConfig config = new SelectByColumnConfig();
        config.setSourceColumn(InterfaceName.AccountId.name());
        config.setDestColumn(InterfaceName.ContactId.name());
        SparkJobResult result = runSparkJob(SelectByColumnJob.class, config, inputs, getWorkspace());
        verifyResult(result);
    }

    protected Boolean verifySingleTarget(HdfsDataUnit tgt) {
        AtomicInteger count = new AtomicInteger();
        Set<String> expectedSet = new HashSet<>(Arrays.asList("ct1", "ct2", "ct4"));
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            count.incrementAndGet();
            System.out.println(record.get(InterfaceName.ContactId.name()));
            Assert.assertTrue(expectedSet.contains(record.get(InterfaceName.ContactId.name()).toString()));
        });
        Assert.assertEquals(count.get(), 3);
        return true;
    }
}
