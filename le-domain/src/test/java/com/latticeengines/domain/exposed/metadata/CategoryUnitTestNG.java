package com.latticeengines.domain.exposed.metadata;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CategoryUnitTestNG {

    @Test(groups = "unit")
    public void testPremium() {
        Assert.assertTrue(Category.INTENT.isPremium());
        Assert.assertTrue(Category.ACCOUNT_ATTRIBUTES.isPremium());
        Assert.assertTrue(Category.CONTACT_ATTRIBUTES.isPremium());
        Assert.assertTrue(Category.TECHNOLOGY_PROFILE.isPremium());
        Assert.assertFalse(Category.FIRMOGRAPHICS.isPremium());
        Assert.assertTrue(Category.GROWTH_TRENDS.isPremium());
    }

}
