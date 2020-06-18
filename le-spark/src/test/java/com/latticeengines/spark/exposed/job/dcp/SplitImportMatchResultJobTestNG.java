package com.latticeengines.spark.exposed.job.dcp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.SplitImportMatchResultConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class SplitImportMatchResultJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final String[] FIELDS = {
            "CustomerID",
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
        Map<String, String> map = Arrays.stream(FIELDS).collect(Collectors.toMap(e->e, e->e));
        config.setAcceptedAttrsMap(map);
        config.setRejectedAttrsMap(map);
        SparkJobResult result = runSparkJob(SplitImportMatchResultJob.class, config);
        verifyResult(result);
    }

    @Override
    protected void verifyOutput(String output) {
        System.out.println(output);
        DataReport.DuplicationReport report = JsonUtils.deserialize(output, DataReport.DuplicationReport.class);
        Assert.assertEquals(report.getDistinctRecords(), Long.valueOf(4));
        Assert.assertEquals(report.getUniqueRecords(), Long.valueOf(2));
        Assert.assertEquals(report.getDuplicateRecords(), Long.valueOf(5));
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
                {"7", "888-056", " ", "Switzerland", "adecco.com", "123456"},
                {"8", "777-056", "Zhejiang", "Ali", "alibaba.com", null}
        };
        uploadHdfsDataUnit(data, fields);
    }
}
