package com.latticeengines.pls.metadata.resolution;

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
}
