package com.latticeengines.domain.exposed.util;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public class AccountExtensionUtilTestNG {

    @Test(groups = "unit")
    public void test() {
        List<String> accountIds = Arrays.asList("Account1", "Account2", "Account3");
        List<String> lookups = Arrays.asList("Lookup1", "Lookup2", "Lookup3");
        String lookupIdColumn = "lookupId";

        FrontEndQuery feq = AccountExtensionUtil.constructFrontEndQuery(null, accountIds, lookupIdColumn, lookups, null,
                true, true);

        Assert.assertNotNull(feq);
        Assert.assertEquals(feq.getSort().getAttributes().size(), 2);
        Assert.assertEquals(feq.getLookups().size(), 3);
        Assert.assertEquals(feq.getMainEntity(), BusinessEntity.Account);
        Assert.assertEquals(((LogicalRestriction) ((LogicalRestriction) feq.getAccountRestriction().getRestriction())
                .getRestrictions().get(0)).getRestrictions().size(), 3);

        feq = AccountExtensionUtil.constructFrontEndQuery(null, accountIds, lookupIdColumn, lookups, null, true, false);

        Assert.assertNotNull(feq);
        Assert.assertEquals(feq.getSort().getAttributes().size(), 2);
        Assert.assertEquals(feq.getLookups().size(), 3);
        Assert.assertEquals(feq.getMainEntity(), BusinessEntity.Account);
        Assert.assertEquals(((LogicalRestriction) ((LogicalRestriction) feq.getAccountRestriction().getRestriction())
                .getRestrictions().get(0)).getRestrictions().size(), 2);

        feq = AccountExtensionUtil.constructFrontEndQuery(null, accountIds, lookupIdColumn, lookups, null, false,
                false);

        Assert.assertNotNull(feq);
        Assert.assertEquals(feq.getSort().getAttributes().size(), 2);
        Assert.assertEquals(feq.getLookups().size(), 3);
        Assert.assertEquals(feq.getMainEntity(), BusinessEntity.Account);
        Assert.assertEquals(((LogicalRestriction) ((LogicalRestriction) feq.getAccountRestriction().getRestriction())
                .getRestrictions().get(0)).getRestrictions().size(), 1);

    }
}
