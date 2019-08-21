package com.latticeengines.domain.exposed.query;

import org.testng.Assert;
import org.testng.annotations.Test;

public class EntityTypeUnitTestNG {

    @Test(groups = "unit")
    public void testEntityType() {
        String feedType = "SomeSystem_AccountData";
        EntityType type = EntityType.matchFeedType(feedType);
        Assert.assertNotNull(type);
        Assert.assertEquals(EntityType.Accounts, type);

        feedType = "System_(0)_ContactData";
        type = EntityType.matchFeedType(feedType);
        Assert.assertNotNull(type);
        Assert.assertEquals(EntityType.Contacts, type);

        feedType = "ProductBundle";
        type = EntityType.matchFeedType(feedType);
        Assert.assertNotNull(type);
        Assert.assertEquals(EntityType.ProductBundles, type);
    }
}
