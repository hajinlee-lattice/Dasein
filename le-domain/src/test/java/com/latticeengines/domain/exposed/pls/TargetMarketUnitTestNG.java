package com.latticeengines.domain.exposed.pls;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.query.ComparisonType;
import com.latticeengines.common.exposed.query.ConcreteRestriction;
import com.latticeengines.common.exposed.util.JsonUtils;

public class TargetMarketUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        TargetMarket market = new TargetMarket();
        market.setAccountFilter(new ConcreteRestriction(false, null, ComparisonType.EQUAL, null));
        market.setContactFilter(new ConcreteRestriction(false, null, ComparisonType.GREATER_OR_EQUAL, null));
        String ser = JsonUtils.serialize(market);
        market = JsonUtils.deserialize(ser, TargetMarket.class);
        Assert.assertNotNull(market.getAccountFilter());
        Assert.assertEquals(market.getAccountFilter().getClass(), ConcreteRestriction.class);
        Assert.assertEquals(market.getContactFilter().getClass(), ConcreteRestriction.class);
        Assert.assertEquals(((ConcreteRestriction) market.getAccountFilter()).getRelation(), ComparisonType.EQUAL);
        Assert.assertEquals(((ConcreteRestriction) market.getContactFilter()).getRelation(),
                ComparisonType.GREATER_OR_EQUAL);
    }
}
