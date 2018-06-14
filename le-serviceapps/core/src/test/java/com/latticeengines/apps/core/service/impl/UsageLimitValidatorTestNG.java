package com.latticeengines.apps.core.service.impl;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.testframework.ServiceAppsFunctionalTestNGBase;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;

public class UsageLimitValidatorTestNG extends ServiceAppsFunctionalTestNGBase {

    @Inject
    private UsageLimitValidator usageLimitValidator;

    @Test(groups = "functional")
    public void testGetLimit() {
        Assert.assertEquals(usageLimitValidator.getLimit("Enrichment"),
                (int) AttrConfigUsageOverview.defaultExportLimit);
        Assert.assertEquals(usageLimitValidator.getLimit("CompanyProfile"),
                (int) AttrConfigUsageOverview.defaultCompanyProfileLimit);
    }

}
