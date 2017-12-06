package com.latticeengines.pls.metadata.resolution;

import java.util.ArrayList;
import java.util.List;

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

        List<String> dateTest = new ArrayList<>();
        dateTest.add("11/4/2016 12:00:00 AM");
        dateTest.add("11/4/2016");
        dateTest.add("11/4/2016 0:00");
        Assert.assertTrue(metadataResolver.isDateTypeColumn(dateTest));
    }
}
