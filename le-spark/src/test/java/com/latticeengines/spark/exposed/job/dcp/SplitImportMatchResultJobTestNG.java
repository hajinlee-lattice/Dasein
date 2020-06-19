package com.latticeengines.spark.exposed.job.dcp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
            DataCloudConstants.ATTR_LDC_DUNS,
            DataCloudConstants.ATTR_COUNTRY,
    };

    @Test(groups = "functional")
    public void test() {
        String input = uploadData();
        SplitImportMatchResultConfig config = new SplitImportMatchResultConfig();
        config.setMatchedDunsAttr(DataCloudConstants.ATTR_LDC_DUNS);
        Map<String, String> map = Arrays.stream(FIELDS).collect(Collectors.toMap(e->e, e->e));
        config.setAcceptedAttrsMap(map);
        config.setRejectedAttrsMap(map);
        config.setMatchedCountryAttr(DataCloudConstants.ATTR_COUNTRY);
        config.setCountryCodeName("LDC_CountryCode");
        config.setTotalCount(8L);
        SparkJobResult result = runSparkJob(SplitImportMatchResultJob.class, config, Collections.singletonList(input),
                getWorkspace());
        verifyResult(result);
    }

    @Override
    protected void verifyOutput(String output) {
        System.out.println(output);
        DataReport report = JsonUtils.deserialize(output, DataReport.class);
        DataReport.DuplicationReport dupReport = report.getDuplicationReport();
        Assert.assertEquals(dupReport.getDistinctRecords(), Long.valueOf(4));
        Assert.assertEquals(dupReport.getUniqueRecords(), Long.valueOf(2));
        Assert.assertEquals(dupReport.getDuplicateRecords(), Long.valueOf(5));

        DataReport.GeoDistributionReport geoReport = report.getGeoDistributionReport();
        Map<String, DataReport.GeoDistributionReport.GeographicalItem> geoMap = geoReport.getGeographicalDistributionMap();
        DataReport.GeoDistributionReport.GeographicalItem item1 = geoMap.get("USA");
        Assert.assertNotNull(item1);
        Assert.assertEquals(item1.getCount(), Long.valueOf(4L));
        DataReport.GeoDistributionReport.GeographicalItem item2 = geoMap.get("China");
        Assert.assertNotNull(item2);
        Assert.assertEquals(item2.getCount(), Long.valueOf(2L));

    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verifySingleTarget, this::verifySingleTarget);
    }

    private String uploadData() {
        List<Pair<String, Class<?>>> fields = new ArrayList<>();
        for (String field : FIELDS) {
            fields.add(Pair.of(field, String.class));
        }

        Object[][] data = new Object[][] {
                {"1", "234-567", "California", "United States", "3i.com", "123456", "USA"},
                {"2", "121-567", "New York", "United States", "3k.com", "234567", "USA"},
                {"3", "123-567", "Illinois", "United States", "abbott.com", "345678", "USA"},
                {"4", "234-888", "Guangdong", "China", "qq.com", "456789", "China"},
                {"5", "222-333", "France", "Paris", "accor.com", "456789", "France"},
                {"6", "666-999", "UC", "United States", "3i.com", "456789", "USA"},
                {"7", "888-056", " ", "Switzerland", "adecco.com", "123456", "Switzerland"},
                {"8", "777-056", "Zhejiang", "China", "alibaba.com", null, "China"}
        };
        return uploadHdfsDataUnit(data, fields);
    }

    @Test(groups = "functional")
    public void testNoDuplicate() {
        String input = uploadDataNoDup();
        SplitImportMatchResultConfig config = new SplitImportMatchResultConfig();
        config.setMatchedDunsAttr(DataCloudConstants.ATTR_LDC_DUNS);
        Map<String, String> map = Arrays.stream(FIELDS).collect(Collectors.toMap(e->e, e->e));
        config.setAcceptedAttrsMap(map);
        config.setRejectedAttrsMap(map);
        SparkJobResult result = runSparkJob(SplitImportMatchResultJob.class, config, Collections.singletonList(input),
                getWorkspace());
        verifyNoDupOutput(result.getOutput());
    }

    private void verifyNoDupOutput(String output) {
        DataReport report = JsonUtils.deserialize(output, DataReport.class);
        DataReport.DuplicationReport dupReport = report.getDuplicationReport();
        Assert.assertEquals(dupReport.getDistinctRecords(), Long.valueOf(7));
        Assert.assertEquals(dupReport.getUniqueRecords(), Long.valueOf(7));
        Assert.assertEquals(dupReport.getDuplicateRecords(), Long.valueOf(0));
    }

    private String uploadDataNoDup() {
        List<Pair<String, Class<?>>> fields = new ArrayList<>();
        for (String field : FIELDS) {
            fields.add(Pair.of(field, String.class));
        }

        Object[][] data = new Object[][] {
                {"1", "234-567", "California", "United States", "3i.com", "123456",  "USA"},
                {"2", "121-567", "New York", "United States", "3k.com", "234567",  "USA"},
                {"3", "123-567", "Illinois", "United States", "abbott.com", "345678",  "USA"},
                {"4", "234-888", "Guangdong", "China", "qq.com", "456789",  "USA"},
                {"5", "222-333", "France", "Paris", "accor.com", "567890",  "USA"},
                {"6", "666-999", "UC", "United States", "3i.com", "678901",  "USA"},
                {"7", "888-056", " ", "Switzerland", "adecco.com", "789012",  "USA"},
                {"8", "777-056", "Zhejiang", "Ali", "alibaba.com", null,  "USA"}
        };
        return uploadHdfsDataUnit(data, fields);
    }
}
