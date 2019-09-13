package com.latticeengines.domain.exposed.query;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class EntityTypeUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "feedTypeCases")
    public void testEntityType(String feedType, EntityType expectType, String expectSystemName) {
        EntityType type = EntityTypeUtils.matchFeedType(feedType);
        Assert.assertEquals(type, expectType);
        Assert.assertEquals(EntityTypeUtils.getSystemName(feedType), expectSystemName);
    }

    @DataProvider(name = "feedTypeCases")
    private Object[][] feedTypeCases() {
        return new Object[][] {
                {"SomeSystem_AccountData", EntityType.Accounts, "SomeSystem"},
                {"System_(0)_ContactData", EntityType.Contacts, "System_(0)"},
                {"ProductBundle", EntityType.ProductBundles, ""},
                {"WebSystem_WebVisitData", EntityType.WebVisit, "WebSystem"},
                {"WebSystem_WebVisitPathPattern", EntityType.WebVisitPathPattern, "WebSystem"},
                {"WrongFeed", null, ""}
        };
    }
}
