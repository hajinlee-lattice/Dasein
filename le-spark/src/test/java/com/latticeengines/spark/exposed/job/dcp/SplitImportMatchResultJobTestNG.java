package com.latticeengines.spark.exposed.job.dcp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.dcp.DunsStats;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.SplitImportMatchResultConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class SplitImportMatchResultJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final String[] FIELDS = {
            "Customer ID",
            InterfaceName.PhoneNumber.name(),
            InterfaceName.State.name(),
            InterfaceName.Country.name(),
            InterfaceName.Website.name(),
            DataCloudConstants.ATTR_LDC_DUNS
    };

    @Test(groups = "functional")
    public void test() {
        uploadData();
        SplitImportMatchResultConfig config = new SplitImportMatchResultConfig();
        config.setMatchedDunsAttr(DataCloudConstants.ATTR_LDC_DUNS);
        config.setAcceptedAttrsMap(Collections.EMPTY_MAP);
        config.setRejectedAttrsMap(Collections.EMPTY_MAP);
        SparkJobResult result = runSparkJob(SplitImportMatchResultJob.class, config);
        verifyResult(result);
    }

    @Override
    protected void verifyOutput(String output) {
        System.out.println(output);
        DunsStats stats = JsonUtils.deserialize(output, DunsStats.class);
        Assert.assertEquals(stats.getDistinctCnt(), Long.valueOf(4));
        Assert.assertEquals(stats.getUniqueCnt(), Long.valueOf(2));
        Assert.assertEquals(stats.getDuplicatedCnt(), Long.valueOf(3));
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verifySingleTarget, this::verifySingleTarget);
    }

    private void uploadData() {
        List<Pair<String, Class<?>>> fields = new ArrayList<>();
        for (String field : FIELDS) {
            fields.add(Pair.of(field, String.class));
        }

        Object[][] data = new Object[][] {
                {"1", "234-567", "California", "United States", "3i.com", "123456"},
                {"2", "121-567", "New York", "United States", "3k.com", "234567"},
                {"3", "123-567", "Illinois", "United States", "abbott.com", "345678"},
                {"4", "234-888", "Guangdong", "China", "qq.com", "456789"},
                {"5", "222-333", "France", "Paris", "accor.com", "456789"},
                {"6", "666-999", "UC", "United States", "3i.com", "456789"},
                {"7", "888-056", " ", "Switzerland", "adecco.com", "123456"}
        };
        uploadHdfsDataUnit(data, fields);
    }
}
