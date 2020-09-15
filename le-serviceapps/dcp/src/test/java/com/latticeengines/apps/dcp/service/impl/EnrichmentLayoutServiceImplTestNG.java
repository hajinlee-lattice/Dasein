package com.latticeengines.apps.dcp.service.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;

public class EnrichmentLayoutServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    EnrichmentLayoutService enrichmentLayoutService;

    @Inject
    TenantService tenantService;

    @BeforeClass(groups = "functional")
    public void setup() {

    }

    @Test(groups = "functional")
    public void testGetAll() {
        String layoutId = "1";
        String userEmail = "user@dnb.com";
        String tenantId = "PropDataService.PropDataService.Production";
        Tenant tenant = tenantService.findByTenantId(tenantId);
        EnrichmentLayout layout = new EnrichmentLayout();
        layout.setLayoutId(layoutId);
        layout.setCreatedBy(userEmail);
        layout.setTenant(tenant);
        List<String> elementList = Arrays.asList("primaryname", "duns_number");
        layout.setElements(elementList);

        enrichmentLayoutService.create(layout);

        List<EnrichmentLayout> all = enrichmentLayoutService.getAll();
        Assert.assertNotNull(all);
        Assert.assertEquals(1, all.size());
        Assert.assertEquals(userEmail, all.get(0).getCreatedBy());
        List<String> resultElementList = all.get(0).getElements();
        Assert.assertNotNull(resultElementList);
        Assert.assertEquals(2, resultElementList.size());

        Boolean result = elementList.stream().map(element -> resultElementList.contains(element)).reduce(Boolean.TRUE,
                (accum, contains) -> accum && contains);
        Assert.assertTrue(result);

        result = elementList.stream().map(resultElementList::contains).reduce(Boolean.TRUE, Boolean::logicalAnd);
        Assert.assertTrue(result);

        result = resultElementList.containsAll(elementList);
        Assert.assertTrue(result);


        enrichmentLayoutService.delete(layoutId);
    }

}
