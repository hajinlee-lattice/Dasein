package com.latticeengines.pls.metadata.resolution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modeling.ModelingMetadata;

public class MetadataResolverUnitTestNG {

    private MetadataResolver metadataResolver = new MetadataResolver();

    @Test(groups = "unit")
    public void testGetCategoryBasedOnSchemaType() {
        Assert.assertEquals(metadataResolver.getCategoryBasedOnSchemaType("SalesforceAccount"),
                ModelingMetadata.CATEGORY_ACCOUNT_INFORMATION);
        Assert.assertEquals(metadataResolver.getCategoryBasedOnSchemaType("SalesforceLead"),
                ModelingMetadata.CATEGORY_LEAD_INFORMATION);
        Assert.assertEquals(metadataResolver.getCategoryBasedOnSchemaType("Category"),
                ModelingMetadata.CATEGORY_LEAD_INFORMATION);
        Assert.assertEquals(metadataResolver.getCategoryBasedOnSchemaType("someRandomeCategory"),
                ModelingMetadata.CATEGORY_LEAD_INFORMATION);
        Assert.assertEquals(metadataResolver.getCategoryBasedOnSchemaType(null),
                ModelingMetadata.CATEGORY_LEAD_INFORMATION);
    }

    @Test(groups = "unit")
    public void testTypeParse() {
        List<String> booleanTest = new ArrayList<>();
        booleanTest.add("1");
        booleanTest.add("0");
        Assert.assertFalse(metadataResolver.isBooleanTypeColumn(booleanTest));

        // case 1: for unified date format
        MutablePair<String, String> formatForDateAndTime = new MutablePair<String, String>();
        Assert.assertTrue(
                metadataResolver.isDateTypeColumn(Arrays.asList("11/4/2016", "12/05/2018"), formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "MM/DD/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 2: malformed date time value, expected false
        Assert.assertFalse(
                metadataResolver.isDateTypeColumn(Arrays.asList("11/41/2016", "12/q@ax/2018", "1q2/q@ax/2019"),
                        formatForDateAndTime));

        // case 3: 55 not in appropriate range of month and date, expected false
        Assert.assertFalse(metadataResolver.isDateTypeColumn(Arrays.asList("11/55/2016", "12/55/2018", "42/05/2018"),
                formatForDateAndTime));

        // case 4: M-d-yyyy and M-d-yyyy have same occurrence times, hit M/d/yyyy => MM/DD/YYYY.
        Assert.assertTrue(
                metadataResolver.isDateTypeColumn(Arrays.asList("11/4/2016", "12/05/2018", "11-4-2016", "12-05-2018"),
                        formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "MM/DD/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 5: M/d/yyyy and d/M/yyyy have same occurrence times, hit M/d/yyyy => MM/DD/YYYY.
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016", "12/05/2018"), formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "MM/DD/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 6: M/d/yyyy 2 times and d/M/yyyy 4 times, hit d/M/yyyy => DD/MM/YYYY.
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016", "12/05/2018", "23/04/2016", "23/05/2018"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "DD/MM/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 7: M-d-yyyy 2 times and d/M/yyyy 2 times, hit d/M/yyy =>
        // DD/MM/YYYY,
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11-4-2016", "12-05-2018", "23/04/2016", "23/05/2018"), formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "DD/MM/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 8: add some timezone, no match pattern, false
        Assert.assertFalse(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016 PM+0800", "02/01/2019 3:20:55 PM+0800", "04/01/2019 3:20:55 PM+0000"),
                formatForDateAndTime));

        // case 9: for unified date time format M/d/yyyy H:m:s => MM/DD/YYYY 00:00:00 24H
        Assert.assertTrue(metadataResolver.isDateTypeColumn(Arrays.asList("11/4/2016 4:20:10", "12/05/2018 20:10:20"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "MM/DD/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), "00:00:00 24H");

        // case 10: same times for M/d/yyyy H:m:s and M/d/yyyy H-m-s, M/d/yyyy H:m:s wins => MM/DD/YYYY 00:00:00 24H
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016 4:20:10", "12/05/2018 20:10:20", "11/4/2016 4-20-10", "12/05/2018 20-10-20"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "MM/DD/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), "00:00:00 24H");

        // case 11: same times for M/d/yyyy and M/d/yyyy H-m-s, M/d/yyyy H-m-s wins => MM/DD/YYYY 00-00-00 24H
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016", "12/05/2018", "11/4/2016 4-20-10", "12/05/2018 20-10-20"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "MM/DD/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), "00-00-00 24H");

        // case 12: identify M/d/yyyy h-m-s a => MM/DD/YYYY 00-00-00 12H
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016", "12/05/2018", "11/4/2016 4-20-10 AM", "12/05/2018 6-10-20 PM"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "MM/DD/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), "00-00-00 12H");

        // case 13: same times for M/d/yyyy and M/d/yyyy H-m-s a, M/d/yyyy H-m-s a => MM/DD/YYYY 00-00-00 12H wins.
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016", "12/05/2018", "11/4/2016 4-20-10 AM", "12/05/2018 6-10-20 PM"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "MM/DD/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), "00-00-00 12H");

        // case 14: missing date time value, skip those and expect true.
        Assert.assertTrue(
                metadataResolver.isDateTypeColumn(
                        Arrays.asList("11/4/2016", "", "12/05/2018", null, "", "01/01/01",
                        "13/11/2017"),
                        formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "DD/MM/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 15: verify two digit year can be recognized
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/16", "", "12/05/18", null, "", "1/1/01", "13/11/17"), formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "DD/MM/YY");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 16: verify four digit year can be recognized
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016", "12/05/1218", null, "1/1/1001", "13/11/2017"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "DD/MM/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 17: same times for year with 2 digit and year with 4 digit, 4
        // digit wins
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016", "15/05/1218", null, "1/1/01", "13/11/17"), formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "DD/MM/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 18: verify 3 characters month
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/Apr/2016", "15/Mar/1218", null, "1/Jan/01", "13/Feb/17"), formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "DD/MMM/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 19: verify existing illegal date type while still be detected as
        // date
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/Apr/2016", "125/Mar/1218", null, "1$/Jan/01", "13%/Feb/17", "2%/May/2019",
                        "21#/Jun/2019", "20%/Jun/2019", "19%/Jun/2019", "1%/Jun/2019"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "DD/MMM/YYYY");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 20: verify existing legal date type while not more than 10%
        // which can't be detected as date
        Assert.assertFalse(
                metadataResolver.isDateTypeColumn(
                        Arrays.asList("11/Apr/2016", "125/Mar/1218", null, null, "1$/Jan/01", "13%/Feb/17",
                                "2%/May/2019", "21#/Jun/2019", "20%/Jun/2019", "19%/Jun/2019", "1%/Jun/2019"),
                        formatForDateAndTime));
    }
}
