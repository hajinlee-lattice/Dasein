package com.latticeengines.datacloud.match.service.impl;

import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.service.DirectPlusEnrichService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.PrimeAccount;

public class DirectPlusEnrichServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Inject
    private DirectPlusEnrichService enrichService;

    @Test(groups = "functional")
    public void testFetchCompInfo() {
        PrimeAccount result = enrichService.fetch(Collections.singleton("060902413")).get(0);
        Assert.assertTrue(StringUtils.isNotBlank(result.getId()));
        Assert.assertNotNull(result.getResult().get("PrimaryBusinessName"));
    }

}
