package com.latticeengines.domain.exposed.pls;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Restriction;

public class TargetMarketUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        TargetMarket market = new TargetMarket();
        market.setAccountFilter(Restriction.builder().let(BusinessEntity.Account, "A").eq("1").build());
        market.setContactFilter(Restriction.builder().let(BusinessEntity.Account, "B").gte(2).build());
        String ser = JsonUtils.serialize(market);
        System.out.println(JsonUtils.pprint(market));
        market = JsonUtils.deserialize(ser, TargetMarket.class);
        Assert.assertNotNull(market.getAccountFilter());
        Assert.assertEquals(market.getAccountFilter().getClass(), ConcreteRestriction.class);
        Assert.assertEquals(market.getContactFilter().getClass(), ConcreteRestriction.class);
        Assert.assertEquals(((ConcreteRestriction) market.getAccountFilter()).getRelation(), ComparisonType.EQUAL);
        Assert.assertEquals(((ConcreteRestriction) market.getContactFilter()).getRelation(),
                ComparisonType.GREATER_OR_EQUAL);
    }
}
