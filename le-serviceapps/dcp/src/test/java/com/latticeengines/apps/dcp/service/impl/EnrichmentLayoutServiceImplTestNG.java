package com.latticeengines.apps.dcp.service.impl;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang.RandomStringUtils;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;
import com.latticeengines.proxy.exposed.matchapi.PrimeMetadataProxy;
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
    public void testCreateValidWithFewElements() {

        PrimeMetadataProxy mockPrimeMetadataProxy = mock(PrimeMetadataProxy.class);
        Set<String> mockResult = new HashSet<>(Arrays.asList("companyinfo_L1_v1"));
        when(mockPrimeMetadataProxy.getBlocksContainingElements(anyList())).thenReturn(mockResult);
        ReflectionTestUtils.setField(enrichmentLayoutService, "primeMetadataProxy", mockPrimeMetadataProxy);

        String layoutId = RandomStringUtils.randomAlphanumeric(4);
        String userEmail = "user@dnb.com";
        String tenantId = "PropDataService.PropDataService.Production";
        EnrichmentLayout layout = new EnrichmentLayout();
        layout.setLayoutId(layoutId);
        layout.setCreatedBy(userEmail);

        layout.setDomain(DataDomain.SalesMarketing);
        layout.setRecordType(DataRecordType.Domain);
        layout.setSourceId("sourceId");
        List<String> elementList = Arrays.asList("primaryname", "duns_number", //
                "dnbassessment_decisionheadqtr_duns","dnbassessment_decisionheadqtr_decisionpowerscore");
        layout.setElements(elementList);

        ResponseDocument<String> result = enrichmentLayoutService.create(tenantId, layout);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isSuccess(), "EnrichmentLayout is not valid.");

        EnrichmentLayoutDetail retrievedLayout = enrichmentLayoutService.findEnrichmentLayoutDetailByLayoutId(mainCustomerSpace, layoutId);

        Assert.assertNotNull(retrievedLayout);
        Assert.assertEquals(retrievedLayout.getDomain(), DataDomain.SalesMarketing);
        Assert.assertEquals(retrievedLayout.getSourceId(), "sourceId");

    }

    @Test(groups = "functional")
    public void testCreateNotValid () {

        PrimeMetadataProxy mockPrimeMetadataProxy = mock(PrimeMetadataProxy.class);
        Set<String> mockResult = new HashSet<>(Arrays.asList("companyinfo_L1_v1", "companyinfo_L2_v1", "companyinfo_L3_v1"));
        when(mockPrimeMetadataProxy.getBlocksContainingElements(anyList())).thenReturn(mockResult);
        ReflectionTestUtils.setField(enrichmentLayoutService, "primeMetadataProxy", mockPrimeMetadataProxy);

        EnrichmentLayout enrichmentLayout = makeEnrichmentLayoutObj( //
                Arrays.asList("primaryname", //
                "duns_number", //
                "thirdpartyassessment_val", //  This element needs companyinfo_L3 which this tenant doesn't have
                "finervicesprospectormodel_totalbalancesegment", //
                "website_url" //
        ));

        ResponseDocument<String> result = enrichmentLayoutService.create(mainTestTenant.getId(), enrichmentLayout);

        Assert.assertNotNull(result);
        Assert.assertFalse(result.isSuccess(), "EnrichmentLayout should not be valid. " +
                "'thirdpartyassessment_val' requires companyinfo level 3");

        EnrichmentLayoutDetail nullLayout = enrichmentLayoutService.findEnrichmentLayoutDetailByLayoutId(mainCustomerSpace, enrichmentLayout.getLayoutId());
        Assert.assertNull(nullLayout, "Layout should be not exists because it failed validation.");
    }

    @Test(groups = "functional")
    public void testUpdateValid () {

        PrimeMetadataProxy mockPrimeMetadataProxy = mock(PrimeMetadataProxy.class);
        Set<String> mockResult = new HashSet<>(Arrays.asList("companyinfo_L1_v1", "companyinfo_L2_v1"));
        when(mockPrimeMetadataProxy.getBlocksContainingElements(anyList())).thenReturn(mockResult);
        ReflectionTestUtils.setField(enrichmentLayoutService, "primeMetadataProxy", mockPrimeMetadataProxy);

        EnrichmentLayout enrichmentLayout = makeEnrichmentLayoutObj( //
                Arrays.asList("primaryname", //
                        "duns_number", //
                        "website_url" //
                ));

        String layoutId = enrichmentLayout.getLayoutId();
        ResponseDocument<String> result = enrichmentLayoutService.create(mainTestTenant.getId(), enrichmentLayout);

        Assert.assertNotNull(result);

        enrichmentLayout.setDomain(DataDomain.Supply);
        enrichmentLayout.setRecordType(DataRecordType.MasterData);
        ResponseDocument<String> r2 = enrichmentLayoutService.update(mainTestTenant.getId(), enrichmentLayout);

        Assert.assertNotNull(r2);
        Assert.assertTrue(r2.isSuccess());

        EnrichmentLayoutDetail updatedLayout = enrichmentLayoutService.findEnrichmentLayoutDetailByLayoutId(mainCustomerSpace, layoutId);
        Assert.assertNotNull(updatedLayout);
        Assert.assertEquals(DataDomain.Supply, updatedLayout.getDomain());

    }

    private EnrichmentLayout makeEnrichmentLayoutObj(List<String> elementList) {

        String layoutId = RandomStringUtils.randomAlphanumeric(8);
        String userEmail = "user@dnb.com";

        EnrichmentLayout layout = new EnrichmentLayout();
        layout.setLayoutId(layoutId);
        layout.setCreatedBy(userEmail);

        layout.setSourceId("sourceId");
        layout.setDomain(DataDomain.SalesMarketing);
        layout.setRecordType(DataRecordType.Domain);

        layout.setElements(elementList);

        return layout;
    }
}
