package com.latticeengines.domain.exposed.metadata;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.SubcategoryStatistics;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class StatisticsUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        AttributeStats attrStats = new AttributeStats();
        attrStats.setNonNullCount(1L);
        AttributeStats attrStats2 = new AttributeStats();
        attrStats2.setNonNullCount(2L);
        AttributeLookup attrLookup = new AttributeLookup(BusinessEntity.Account, "Name");

        SubcategoryStatistics subcategoryStats = new SubcategoryStatistics();
        Map<AttributeLookup, AttributeStats> attrs = new HashMap<>();
        attrs.put(attrLookup, attrStats);
        attrs.put(attrLookup, attrStats2);
        subcategoryStats.setAttributes(attrs);

        CategoryStatistics categoryStatistics = new CategoryStatistics();
        Map<String, SubcategoryStatistics> subCats = new HashMap<>();
        subCats.put("Other", subcategoryStats);
        categoryStatistics.setSubcategories(subCats);

        Statistics stats = new Statistics();
        Map<Category, CategoryStatistics> cats = new HashMap<>();
        cats.put(Category.FIRMOGRAPHICS, categoryStatistics);
        stats.setCategories(cats);
        stats.updateCount();

        String serialized = JsonUtils.serialize(stats);

        Statistics deserialized = JsonUtils.deserialize(serialized, Statistics.class);
        Assert.assertEquals(deserialized.getCount(), new Long(2L));
    }


}
