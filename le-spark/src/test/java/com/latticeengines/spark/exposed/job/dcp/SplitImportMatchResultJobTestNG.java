package com.latticeengines.spark.exposed.job.dcp;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.SplitImportMatchResultConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class SplitImportMatchResultJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final List<Pair<String, Class<?>>> FIELDS = Arrays.asList(
            Pair.of("CustomerID", String.class),
            Pair.of(InterfaceName.PhoneNumber.name(), String.class),
            Pair.of(InterfaceName.State.name(), String.class),
            Pair.of(InterfaceName.Country.name(), String.class),
            Pair.of(InterfaceName.Website.name(), String.class),
            Pair.of(DataCloudConstants.ATTR_LDC_DUNS, String.class),
            Pair.of("LDC_ConfidenceCode", Integer.class)
    );

    @Value("${datacloud.manage.url}")
    private String url;

    @Value("${datacloud.manage.user}")
    private String user;

    @Value("${datacloud.manage.password.encrypted}")
    private String password;

    @Test(groups = "functional")
    public void test() {
        List<Runnable> threads = Arrays.asList(this::testDataReport, this::testNoDunsDuplicate);
        ThreadPoolUtils.runInParallel(threads);
    }

    public void testDataReport() {
        String input = uploadData();
        SplitImportMatchResultConfig config = new SplitImportMatchResultConfig();
        config.setMatchedDunsAttr(DataCloudConstants.ATTR_LDC_DUNS);
        Map<String, String> map = FIELDS.stream().map(Pair::getLeft).collect(Collectors.toMap(e->e, e->e));
        config.setAcceptedAttrsMap(map);
        config.setRejectedAttrsMap(map);
        config.setCountryAttr(InterfaceName.Country.name());
        config.setUrl(url);
        config.setUser(user);
        String key = CipherUtils.generateKey();
        config.setEncryptionKey(key);
        String salt = CipherUtils.generateKey();
        config.setSaltHint(salt);
        config.setPassword(CipherUtils.encrypt(password, key, salt));
        config.setConfidenceCodeAttr( "LDC_ConfidenceCode");
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
        DataReport.GeoDistributionReport.GeographicalItem item1 = geoMap.get("US");
        Assert.assertNotNull(item1);
        Assert.assertEquals(item1.getCount(), Long.valueOf(4L));
        DataReport.GeoDistributionReport.GeographicalItem item2 = geoMap.get("CN");
        Assert.assertNotNull(item2);
        Assert.assertEquals(item2.getCount(), Long.valueOf(2L));

        DataReport.MatchToDUNSReport matchToDUNSReport = report.getMatchToDUNSReport();
        Assert.assertNotNull(matchToDUNSReport);
    }

    @Override
    protected List<Function<HdfsDataUnit, Boolean>> getTargetVerifiers() {
        return Arrays.asList(this::verifySingleTarget, this::verifySingleTarget);
    }

    private String uploadData() {
        Object[][] data = new Object[][] {
                {"1", "234-567", "California", "United States", "3i.com", "123456", 1},
                {"2", "121-567", "New York", "United States", "3k.com", "234567", 2},
                {"3", "123-567", "Illinois", "United States", "abbott.com", "345678", 3},
                {"4", "234-888", "Guangdong", "China", "qq.com", "456789", 4},
                {"5", "222-333", "Paris", "France", "accor.com", "456789", 5},
                {"6", "666-999", "UC", "United States", "3i.com", "456789", 6},
                {"7", "888-056", " ", "Switzerland", "adecco.com", "123456", 7},
                {"8", "777-056", "Zhejiang", "China", "alibaba.com", null, 0}
        };
        return uploadHdfsDataUnit(data, FIELDS);
    }

    public void testNoDunsDuplicate() {
        String input = uploadDataNoDup();
        SplitImportMatchResultConfig config = new SplitImportMatchResultConfig();
        config.setMatchedDunsAttr(DataCloudConstants.ATTR_LDC_DUNS);
        config.setConfidenceCodeAttr( "LDC_ConfidenceCode");
        config.setTotalCount(8L);
        Map<String, String> map = FIELDS.stream().map(Pair::getLeft).collect(Collectors.toMap(e->e, e->e));
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

        Object[][] data = new Object[][] {
                {"1", "234-567", "California", "United States", "3i.com", "123456", 1},
                {"2", "121-567", "New York", "United States", "3k.com", "234567", 2},
                {"3", "123-567", "Illinois", "United States", "abbott.com", "345678", 3},
                {"4", "234-888", "Guangdong", "China", "qq.com", "456789", 4},
                {"5", "222-333", "France", "Paris", "accor.com", "567890", 5},
                {"6", "666-999", "UC", "United States", "3i.com", "678901", 6},
                {"7", "888-056", " ", "Switzerland", "adecco.com", "789012", 7},
                {"8", "777-056", "Zhejiang", "Ali", "alibaba.com", null, 8}
        };
        return uploadHdfsDataUnit(data, FIELDS);
    }
}
