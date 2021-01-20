package com.latticeengines.domain.exposed.metadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CategoryUnitTestNG {

    @Test(groups = "unit")
    public void testPremium() {
        Assert.assertTrue(Category.INTENT.isPremium());
        Assert.assertTrue(Category.ACCOUNT_ATTRIBUTES.isPremium());
        Assert.assertTrue(Category.CONTACT_ATTRIBUTES.isPremium());
        Assert.assertTrue(Category.TECHNOLOGY_PROFILE.isPremium());
        Assert.assertTrue(Category.DNB_TECHNOLOGY_PROFILE.isPremium());
        Assert.assertFalse(Category.FIRMOGRAPHICS.isPremium());
        Assert.assertTrue(Category.GROWTH_TRENDS.isPremium());
        Assert.assertTrue(Category.COVID_19.isPremium());
    }

    @Test(groups = "unit")
    private void testNoDuplicateOrder() {
        Map<Integer, Set<Category>> orderCategories = new HashMap<>();
        for (Category category : Category.values()) {
            Integer order = category.getOrder();
            Assert.assertNotNull(order, String.format("%s have null order", category));

            orderCategories.putIfAbsent(order, new HashSet<>());
            orderCategories.get(order).add(category);
        }

        // no two categories should have the same order
        orderCategories.forEach((order, categories) -> Assert.assertEquals(categories.size(), 1,
                String.format("%s have duplicate order %d", categories, order)));
    }

}
