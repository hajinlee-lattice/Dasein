package com.latticeengines.domain.exposed.query;

import org.testng.Assert;
import org.testng.annotations.Test;

public class EntityTypeUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testEntityType() {
        String feedType = "SomeSystem_AccountData";
        EntityType type = EntityTypeUtils.matchFeedType(feedType);
        Assert.assertNotNull(type);
        Assert.assertEquals(EntityType.Accounts, type);

        feedType = "System_(0)_ContactData";
        type = EntityTypeUtils.matchFeedType(feedType);
        Assert.assertNotNull(type);
        Assert.assertEquals(EntityType.Contacts, type);

        feedType = "ProductBundle";
        type = EntityTypeUtils.matchFeedType(feedType);
        Assert.assertNotNull(type);
        Assert.assertEquals(EntityType.ProductBundles, type);

        feedType = "WebSystem_WebVisitData";
        type = EntityTypeUtils.matchFeedType(feedType);
        Assert.assertNotNull(type);
        Assert.assertEquals(EntityType.WebVisit, type);

        feedType = "WebSystem_WebVisitPathPattern";
        type = EntityTypeUtils.matchFeedType(feedType);
        Assert.assertNotNull(type);
        Assert.assertEquals(EntityType.WebVisitPathPattern, type);

        feedType = "WrongFeed";
        type = EntityTypeUtils.matchFeedType(feedType);
        Assert.assertNull(type);
    }
}
