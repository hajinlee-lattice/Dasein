package com.latticeengines.query.evaluator.lookup;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.querydsl.core.types.dsl.ComparableExpression;

public class ResolverTestNG extends QueryFunctionalTestNGBase {
    protected LookupResolverFactory resolverFactory;

    @BeforeClass(groups = "functional")
    public void setupResolverFactory() {
        resolverFactory = new LookupResolverFactory(attrRepo, queryProcessor, "segment");
    }

    @Test(groups = "functional")
    public void testRangeResolver() {
        ConcreteRestriction dBtwn0And10 = (ConcreteRestriction) Restriction.builder() //
                .let(BusinessEntity.Account, "BmbrSurge_BusinessPlanning_Intent") //
                .in(0, 10) //
                .build();

        // RangeLookup rangeLookup = new RangeLookup("10", "100");
        Lookup rangeLookup = dBtwn0And10.getRhs();
        LookupResolver rangeResolver = resolverFactory.getLookupResolver(rangeLookup.getClass());
        List<ComparableExpression> ranges = rangeResolver.resolveForCompare(rangeLookup);
        String range = ranges.get(0).lt(ranges.get(1)).toString();
        Assert.assertTrue(range.contains("0 < 10"), String.format("Expected 0 < 10 instead of %s.", range));

        verify(dBtwn0And10);
    }

    private void verify(Restriction restriction) {
        String serialized = JsonUtils.serialize(restriction);
        System.out.println(serialized);
        Assert.assertNotNull(serialized);
        Assert.assertNotNull(restriction);
    }
}
