package com.latticeengines.domain.exposed.query;

import org.testng.Assert;
import org.testng.annotations.Test;

public class AttributeLookupUnitTestNG {

    @Test(groups = "unit")
    public void testFromString() {

        String attrLookupStr = "account.acct_custom_tp30_6a5a8b926b";
        AttributeLookup lookup = AttributeLookup.fromString(attrLookupStr);
        Assert.assertEquals(lookup.getEntity(), BusinessEntity.Account);

        String attrLookupStr2 = "AccounT.acct_custom_tp30_6a5a8b926b";
        AttributeLookup lookup2 = AttributeLookup.fromString(attrLookupStr);
        Assert.assertEquals(lookup2.getEntity(), BusinessEntity.Account);
    }


}
