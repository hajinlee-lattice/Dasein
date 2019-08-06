package com.latticeengines.domain.exposed.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy.Template;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class RestrictionUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testInspectBucketRestriction() {
        Map<ComparisonType, Set<AttributeLookup>> map = new HashMap<>();
        BucketRestriction br1 = new BucketRestriction();
        br1.setAttr(new AttributeLookup(BusinessEntity.Account, "attr1"));
        Bucket bkt1 = new Bucket();
        bkt1.setDateFilter(TimeFilter.latestDay());
        br1.setBkt(bkt1);
        TimeFilterTranslator tft = new TimeFilterTranslator(ImmutableList.of( //
                new PeriodStrategy(Template.Week), //
                new PeriodStrategy(Template.Month), //
                new PeriodStrategy(Template.Quarter), //
                new PeriodStrategy(Template.Year)), "2019-03-25");

        RestrictionUtils.inspectBucketRestriction(br1, map, tft);
        Assert.assertEquals(map.size(), 1);
        Assert.assertEquals(map.get(ComparisonType.LATEST_DAY).size(), 1);

        BucketRestriction br2 = new BucketRestriction();
        br2.setAttr(new AttributeLookup(BusinessEntity.Contact, "attr2"));
        Bucket bkt2 = new Bucket();
        bkt2.setDateFilter(TimeFilter.latestDay());
        br2.setBkt(bkt2);
        RestrictionUtils.inspectBucketRestriction(br2, map, tft);
        Assert.assertEquals(map.size(), 1);
        Assert.assertEquals(map.get(ComparisonType.LATEST_DAY).size(), 2);

        Map<AttributeLookup, List<Object>> latestDayValues = tft.getSpecifiedValues().get(ComparisonType.LATEST_DAY);
        latestDayValues.put(new AttributeLookup(BusinessEntity.Account, "attr3"), Arrays.asList(1, 2));
        BucketRestriction br3 = new BucketRestriction();
        br3.setAttr(new AttributeLookup(BusinessEntity.Account, "attr3"));
        Bucket bkt3 = new Bucket();
        bkt3.setDateFilter(TimeFilter.latestDay());
        br3.setBkt(bkt3);
        RestrictionUtils.inspectBucketRestriction(br3, map, tft);
        Assert.assertEquals(map.size(), 1);
        Assert.assertEquals(map.get(ComparisonType.LATEST_DAY).size(), 2);
    }

    @Test(groups = "unit", dataProvider = "numericalBuckets")
    public void testConvertNumericalBucketRestriction(List<Object> vals, Class<?> attrClz, boolean exception) {
        if (exception) {
            Assert.assertThrows(() -> RestrictionUtils.convertNumericalValues(vals, attrClz));
        } else {
            List<Object> newVals = RestrictionUtils.convertNumericalValues(vals, attrClz);
            for (Object val: newVals) {
                Assert.assertTrue(Number.class.isAssignableFrom(val.getClass()), JsonUtils.serialize(vals));
            }
        }
    }

    @DataProvider(name = "numericalBuckets")
    public Object[][] provideNumericalBuckets() {
        return new Object[][] {
                { Arrays.asList("1", "2"), Integer.class, false },
                { Arrays.asList("1", "2"), Long.class, false },
                { Arrays.asList("1.2", "2"), Double.class, false },
                { Arrays.asList("1", "2."), Float.class, false },
                { Arrays.asList(1, "2"), Integer.class, false },
                { Arrays.asList("1", 2), Long.class, false },
                { Arrays.asList("1.2", 2), Double.class, false },
                { Arrays.asList("1", 2.D), Float.class, false },
                { Arrays.asList(">1", "2"), Integer.class, true }
        };
    }

}
