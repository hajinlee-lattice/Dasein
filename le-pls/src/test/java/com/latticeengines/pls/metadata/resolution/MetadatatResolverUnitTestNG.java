package com.latticeengines.pls.metadata.resolution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modeling.ModelingMetadata;

public class MetadatatResolverUnitTestNG {

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
        Assert.assertEquals(formatForDateAndTime.getLeft(), "M/d/yyyy");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 2: malformed date time value, expected false
        Assert.assertFalse(
                metadataResolver.isDateTypeColumn(Arrays.asList("11/4/2016", "12/q@ax/2018", "12/05/2018"),
                        formatForDateAndTime));

        // case 3: 55 not in appropriate range of month and date, expected false
        Assert.assertFalse(metadataResolver.isDateTypeColumn(Arrays.asList("11/4/2016", "12/55/2018", "12/05/2018"),
                formatForDateAndTime));

        // case 4: M-d-yyyy and M-d-yyyy have same occurrence times, hit
        // M/d/yyyy
        Assert.assertTrue(
                metadataResolver.isDateTypeColumn(Arrays.asList("11/4/2016", "12/05/2018", "11-4-2016", "12-05-2018"),
                        formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "M/d/yyyy");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 5: M/d/yyyy and d/M/yyyy have same occurrence times, hit
        // M/d/yyyy
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016", "12/05/2018"), formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "M/d/yyyy");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 6: M/d/yyyy 2 times and d/M/yyyy 4 times, hit d/M/yyyy
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016", "12/05/2018", "23/04/2016", "23/05/2018"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "d/M/yyyy");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 7: M-d-yyyy 2 times and d/M/yyyy 2 times, hit M-d-yyyy
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11-4-2016", "12-05-2018", "23/04/2016", "23/05/2018"), formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "M-d-yyyy");
        Assert.assertEquals(formatForDateAndTime.getRight(), null);

        // case 8: add some timezone, no match pattern, false
        Assert.assertFalse(metadataResolver.isDateTypeColumn(Arrays.asList("11/4/2016", "02/01/2019 3:20:55 PM+0800"),
                formatForDateAndTime));

        // case 9: for unified date time format M/d/yyyy H:m:s
        Assert.assertTrue(metadataResolver.isDateTypeColumn(Arrays.asList("11/4/2016 4:20:10", "12/05/2018 20:10:20"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "M/d/yyyy");
        Assert.assertEquals(formatForDateAndTime.getRight(), "H:m:s");

        // case 10: same times for M/d/yyyy H:m:s and M/d/yyyy H-m-s
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016 4:20:10", "12/05/2018 20:10:20", "11/4/2016 4-20-10", "12/05/2018 20-10-20"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "M/d/yyyy");
        Assert.assertEquals(formatForDateAndTime.getRight(), "H:m:s");

        // case 11: same times for M/d/yyyy and M/d/yyyy H-m-s
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016", "12/05/2018", "11/4/2016 4-20-10", "12/05/2018 20-10-20"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "M/d/yyyy");
        Assert.assertEquals(formatForDateAndTime.getRight(), "H-m-s");

        // case 12: identify M/d/yyyy h-m-s a
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016", "12/05/2018", "11/4/2016 4-20-10 AM", "12/05/2018 6-10-20 PM"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "M/d/yyyy");
        Assert.assertEquals(formatForDateAndTime.getRight(), "h-m-s a");

        // case 13: same times for M/d/yyyy and M/d/yyyy H-m-s a
        Assert.assertTrue(metadataResolver.isDateTypeColumn(
                Arrays.asList("11/4/2016", "12/05/2018", "11/4/2016 4-20-10 AM", "12/05/2018 6-10-20 PM"),
                formatForDateAndTime));
        Assert.assertEquals(formatForDateAndTime.getLeft(), "M/d/yyyy");
        Assert.assertEquals(formatForDateAndTime.getRight(), "h-m-s a");

    }
}
