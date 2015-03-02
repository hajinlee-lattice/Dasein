package com.latticeengines.domain.exposed.camille;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CustomerSpaceUnitTestNG {

    @Test(groups = "unit")
    public void testParseBackwardsCompatible() {
        CustomerSpace space = CustomerSpace.parse("Foo");
        Assert.assertEquals(space.getContractId(), "Foo");
        Assert.assertEquals(space.getTenantId(), "Foo");
        Assert.assertEquals(CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, space.getSpaceId());
    }
}
