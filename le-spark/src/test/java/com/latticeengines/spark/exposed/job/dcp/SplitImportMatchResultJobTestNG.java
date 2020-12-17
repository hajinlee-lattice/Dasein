package com.latticeengines.spark.exposed.job.dcp;

import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.Classification;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.ConfidenceCode;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Attr.MatchedDuns;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.dcp.SplitImportMatchResultConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class SplitImportMatchResultJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SplitImportMatchResultJobTestNG.class);

    private static final List<Pair<String, Class<?>>> FIELDS = Arrays.asList(
            Pair.of("CustomerID", String.class),
            Pair.of(InterfaceName.PhoneNumber.name(), String.class),
            Pair.of(InterfaceName.State.name(), String.class),
            Pair.of(InterfaceName.Country.name(), String.class),
            Pair.of(InterfaceName.Website.name(), String.class),
            Pair.of(MatchedDuns, String.class),
            Pair.of(Classification, String.class),
            Pair.of(ConfidenceCode, Integer.class),
            Pair.of("Organization", String.class),
            Pair.of(MatchConstants.MATCH_ERROR_TYPE, String.class),
            Pair.of(MatchConstants.MATCH_ERROR_CODE, String.class)
    );

    @Value("${datacloud.manage.url}")
    private String url;

    @Value("${datacloud.manage.user}")
    private String user;

    @Value("${datacloud.manage.password.encrypted}")
    private String password;

    @Test(groups = "functional")
    public void test() {
        testDataReport();
        testNoDunsDuplicate();
    }

    public void testDataReport() {
        String input = uploadData();
        SplitImportMatchResultConfig config = new SplitImportMatchResultConfig();
        config.setMatchedDunsAttr(MatchedDuns);
        config.setClassificationAttr(Classification);
        config.setConfidenceCodeAttr(ConfidenceCode);
        Map<String, String> map =
                new LinkedHashMap<>(FIELDS.stream().map(Pair::getLeft).collect(Collectors.toMap(e->e, e->e)));
        map.put("Organization", "Organization - No. Of Employees - Employee Figures Date");
        List<String> attrs = new ArrayList<>(map.keySet());
        config.setDisplayNameMap(map);
        config.setAcceptedAttrs(attrs);
        config.setRejectedAttrs(attrs);
        config.setErrorIndicatorAttr(MatchConstants.MATCH_ERROR_TYPE);
        config.setErrorCodeAttr(MatchConstants.MATCH_ERROR_CODE);
        config.setCountryAttr(InterfaceName.Country.name());
        config.setManageDbUrl(url);
        config.setUser(user);
        config.setIgnoreErrors(Collections.emptyMap());
        String key = CipherUtils.generateKey();
        config.setEncryptionKey(key);
        String salt = CipherUtils.generateKey();
        config.setSaltHint(salt);
        config.setPassword(CipherUtils.encrypt(password, key, salt));
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
        return Arrays.asList(this::verifySingleTarget, this::verifySingleTarget, this::verifyThirdTarget, this::verifySingleTarget);
    }

    public Boolean verifyThirdTarget(HdfsDataUnit unit) {
        verifyAndReadTarget(unit).forEachRemaining(record -> {
            log.info(String.valueOf(record));
        });
        return true;
    }

    private String uploadData() {
        String accepted = DnBMatchCandidate.Classification.Accepted.name();
        String rejected = DnBMatchCandidate.Classification.Rejected.name();
        Object[][] data = new Object[][]{
                {"1", "234-567", "California", "United States", "3i.com", "123456", accepted, 1, "1-2-3", "", ""},
                {"2", "121-567", "New York", "United States", "3k.com", "234567", accepted, 2, "1-2-3", "", ""},
                {"3", "123-567", "Illinois", "United States", "abbott.com", "345678", accepted, 3, "1-2-3", "", ""},
                {"4", "234-888", "Guangdong", "China", "qq.com", "456789", accepted, 4, "1-2-3", "", ""},
                {"5", "222-333", "Paris", "France", "accor.com", "456789", accepted, 5, "1-2-3", "", ""},
                {"6", "666-999", "UC", "United States", "3i.com", "456789", accepted, 6, "1-2-3", "", ""},
                {"7", "888-056", " ", null, "adecco.com", "123456", accepted, 7, "1-2-3", "", ""},
                {"8", "777-056", "Zhejiang", "China", "alibaba.com", null, rejected, 0, "1-2-3", "", ""}
        };
        return uploadHdfsDataUnit(data, FIELDS);
    }

    public void testNoDunsDuplicate() {
        String input = uploadDataNoDup();
        SplitImportMatchResultConfig config = new SplitImportMatchResultConfig();
        config.setMatchedDunsAttr(MatchedDuns);
        config.setClassificationAttr(Classification);
        config.setConfidenceCodeAttr(ConfidenceCode);
        config.setErrorIndicatorAttr(MatchConstants.MATCH_ERROR_TYPE);
        config.setErrorCodeAttr(MatchConstants.MATCH_ERROR_CODE);
        config.setTotalCount(8L);
        config.setIgnoreErrors(Collections.emptyMap());
        Map<String, String> map = FIELDS.stream().map(Pair::getLeft).collect(Collectors.toMap(e->e, e->e));
        List<String> attrs = new ArrayList<>(map.keySet());
        config.setDisplayNameMap(map);
        config.setAcceptedAttrs(attrs);
        config.setRejectedAttrs(attrs);
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
        String accepted = DnBMatchCandidate.Classification.Accepted.name();
        String rejected = DnBMatchCandidate.Classification.Rejected.name();
        Object[][] data = new Object[][]{
                {"1", "234-567", "California", "United States", "3i.com", "123456", accepted, 1, "1-2-3", "", ""},
                {"2", "121-567", "New York", "United States", "3k.com", "234567", accepted, 2, "1-2-3", "", ""},
                {"3", "123-567", "Illinois", "United States", "abbott.com", "345678", accepted, 3, "1-2-3", "", ""},
                {"4", "234-888", "Guangdong", "China", "qq.com", "456789", accepted, 4, "1-2-3", "", ""},
                {"5", "222-333", "France", "Paris", "accor.com", "567890", accepted, 5, "1-2-3", "", ""},
                {"6", "666-999", "UC", "United States", "3i.com", "678901", accepted, 6, "1-2-3", "", ""},
                {"7", "888-056", " ", "Switzerland", "adecco.com", "789012", accepted, 7, "1-2-3", "", ""},
                {"8", "777-056", "Zhejiang", "Ali", "alibaba.com", null, rejected, 8, "1-2-3", "", ""}
        };
        return uploadHdfsDataUnit(data, FIELDS);
    }
}
