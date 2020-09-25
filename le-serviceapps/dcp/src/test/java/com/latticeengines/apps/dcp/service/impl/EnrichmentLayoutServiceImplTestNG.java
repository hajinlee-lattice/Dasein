package com.latticeengines.apps.dcp.service.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutOperationResult;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;

public class EnrichmentLayoutServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    EnrichmentLayoutService enrichmentLayoutService;

    @Inject
    TenantService tenantService;

    private static final String subscriberNumberWithManyDomains = "202007101";

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        mainTestTenant.setSubscriberNumber(subscriberNumberWithManyDomains);
        tenantService.updateTenant(mainTestTenant);
    }

    @Test(groups = "functional")
    public void testCreateValid() {

        String layoutId = RandomStringUtils.randomAlphanumeric(4);
        String userEmail = "user@dnb.com";
        String tenantId = "PropDataService.PropDataService.Production";
        Tenant tenant = tenantService.findByTenantId(tenantId);
        EnrichmentLayout layout = new EnrichmentLayout();
        layout.setLayoutId(layoutId);
        layout.setCreatedBy(userEmail);
        layout.setTenant(tenant);
        layout.setDomain(DataDomain.SalesMarketing);
        layout.setRecordType(DataRecordType.Domain);
        List<String> elementList = Arrays.asList("primaryname", "duns_number");
        layout.setElements(elementList);

        EnrichmentLayoutOperationResult result = enrichmentLayoutService.create(layout);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isValid(), "EnrichmentLayout is not valid.");

        EnrichmentLayout retrievedLayout = enrichmentLayoutService.findByLayoutId(layoutId);

        Assert.assertNotNull(retrievedLayout);
        Assert.assertEquals(retrievedLayout.getDomain(), DataDomain.SalesMarketing);

    }

    @Test(groups = "functional")
    public void testCreateNotValid () {
        EnrichmentLayout enrichmentLayout = makeEnrichmentLayoutObj( //
                mainTestTenant.getId(), //
                Arrays.asList("primaryname", //
                "duns_number", //
                "thirdpartyassessment_val", //  This element needs companyinfo_L3 which this tenant doesn't have
                "finervicesprospectormodel_totalbalancesegment", //
                "website_url" //
        ));

        EnrichmentLayoutOperationResult result = enrichmentLayoutService.create(enrichmentLayout);

        Assert.assertNotNull(result);
        Assert.assertFalse(result.isValid(), "EnrichmentLayout should not be valid. " +
                "'thirdpartyassessment_val' requires companyinfo level 3");

        EnrichmentLayout nullLayout = enrichmentLayoutService.findByLayoutId(enrichmentLayout.getLayoutId());
        Assert.assertNull(nullLayout, "Layout should be not exists because it failed validation.");
    }

    @Test(groups = "functional")
    public void testUpdateValid () {
        EnrichmentLayout enrichmentLayout = makeEnrichmentLayoutObj( //
                mainTestTenant.getId(), //
                Arrays.asList("primaryname", //
                        "duns_number", //
                        "website_url" //
                ));

        String layoutId = enrichmentLayout.getLayoutId();
        EnrichmentLayoutOperationResult result = enrichmentLayoutService.create(enrichmentLayout);

        Assert.assertNotNull(result);

        enrichmentLayout.setDomain(DataDomain.Supply);
        enrichmentLayout.setRecordType(DataRecordType.MasterData);
        EnrichmentLayoutOperationResult r2 = enrichmentLayoutService.update(enrichmentLayout);

        Assert.assertNotNull(r2);
        Assert.assertTrue(r2.isValid());

        EnrichmentLayout updatedLayout = enrichmentLayoutService.findByLayoutId(layoutId);
        Assert.assertNotNull(updatedLayout);
        Assert.assertEquals(DataDomain.Supply, updatedLayout.getDomain());

    }

    private EnrichmentLayout makeEnrichmentLayoutObj(String tenantId, List<String> elementList) {

        String layoutId = RandomStringUtils.randomAlphanumeric(4);
        String userEmail = "user@dnb.com";
        Tenant tenant = tenantService.findByTenantId(tenantId);
        EnrichmentLayout layout = new EnrichmentLayout();
        layout.setLayoutId(layoutId);
        layout.setCreatedBy(userEmail);
        layout.setTenant(tenant);
        layout.setDomain(DataDomain.SalesMarketing);
        layout.setRecordType(DataRecordType.Domain);

        layout.setElements(elementList);

        return layout;
    }
}
