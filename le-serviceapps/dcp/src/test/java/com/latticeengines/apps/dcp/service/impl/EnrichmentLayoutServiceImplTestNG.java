package com.latticeengines.apps.dcp.service.impl;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang.RandomStringUtils;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.dcp.service.EnrichmentLayoutService;
import com.latticeengines.apps.dcp.testframework.DCPFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;
import com.latticeengines.proxy.exposed.matchapi.PrimeMetadataProxy;
import com.latticeengines.security.exposed.service.TenantService;

public class EnrichmentLayoutServiceImplTestNG extends DCPFunctionalTestNGBase {

    @Inject
    private EnrichmentLayoutService enrichmentLayoutService;

    @Inject
    private TenantService tenantService;

    private static final String sourceIdTemplate = "Source_%s";

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        mainTestTenant.setSubscriberNumber(SUBSRIBER_NUMBER_MANY_DOMAINS);
        tenantService.updateTenant(mainTestTenant);
    }

    private String getRandomSourceId () {
        return String.format(sourceIdTemplate, RandomStringUtils.randomAlphanumeric(6));
    }

    @Test(groups = "functional")
    public void testCreateValidWithFewElements() {

        PrimeMetadataProxy mockPrimeMetadataProxy = mock(PrimeMetadataProxy.class);
        Set<String> mockResult = new HashSet<>(Arrays.asList("companyinfo_L1_v1"));  // This mock is the min set of datablocks that the user needs for the layout to be valid.
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
        String rndSrcId = getRandomSourceId();
        layout.setSourceId(rndSrcId);
        List<String> elementList = Arrays.asList("primaryname", "duns_number", //
                "dnbassessment_decisionheadqtr_duns","dnbassessment_decisionheadqtr_decisionpowerscore");
        layout.setElements(elementList);

        ResponseDocument<String> result = enrichmentLayoutService.create(tenantId, layout);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isSuccess(), "EnrichmentLayout is not valid.");

        RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                Collections.singleton(AssertionError.class), null);
        EnrichmentLayoutDetail retrievedLayout = retry.execute(ctx -> {
            EnrichmentLayoutDetail detail = enrichmentLayoutService.findEnrichmentLayoutDetailByLayoutId(mainCustomerSpace, layoutId);
            Assert.assertNotNull(detail);
            return detail;
        });
        Assert.assertEquals(retrievedLayout.getDomain(), DataDomain.SalesMarketing);
        Assert.assertEquals(retrievedLayout.getSourceId(), rndSrcId);

    }

    @Test(groups = "functional")
    public void testCreateNotValid () {

        PrimeMetadataProxy mockPrimeMetadataProxy = mock(PrimeMetadataProxy.class);
        Set<String> mockResult = new HashSet<>(Arrays.asList("companyinfo_L1_v1", "companyinfo_L2_v1", "companyinfo_L3_v1"));  // User needs at least these
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

        RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                Collections.singleton(AssertionError.class), null);
        retry.execute(ctx -> {
            EnrichmentLayoutDetail nullLayout = enrichmentLayoutService.findEnrichmentLayoutDetailByLayoutId(mainCustomerSpace, enrichmentLayout.getLayoutId());
            Assert.assertNull(nullLayout, "Layout should be not exists because it failed validation.");
            return true;
        });
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

        RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                Collections.singleton(AssertionError.class), null);
        EnrichmentLayoutDetail updatedLayout = retry.execute(ctx -> {
            EnrichmentLayoutDetail detail = enrichmentLayoutService.findEnrichmentLayoutDetailByLayoutId(mainCustomerSpace, layoutId);
            Assert.assertNotNull(detail);
            return detail;
        });
        Assert.assertEquals(DataDomain.Supply, updatedLayout.getDomain());
    }

    @Test(groups = "functional")
    public void testDeleteByLayoutId () {
        PrimeMetadataProxy mockPrimeMetadataProxy = mock(PrimeMetadataProxy.class);
        Set<String> mockResult = new HashSet<>(Arrays.asList("companyinfo_L1_v1", "companyinfo_L2_v1", "companyinfo_L3_v1"));  // User needs at least these
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

        // Now delete it
        enrichmentLayoutService.deleteLayoutByLayoutId(mainTestTenant.getId(), enrichmentLayout.getLayoutId());

        // Check that it's gone
        EnrichmentLayout removedLayout = enrichmentLayoutService.findByLayoutId(mainTestTenant.getId(), enrichmentLayout.getLayoutId());
        Assert.assertNull(removedLayout, "EnrichmentLayout should have been removed.");
    }

    @Test(groups = "functional")
    public void testDeleteBySourceId () {
        PrimeMetadataProxy mockPrimeMetadataProxy = mock(PrimeMetadataProxy.class);
        Set<String> mockResult = new HashSet<>(Arrays.asList("companyinfo_L1_v1", "companyinfo_L2_v1", "companyinfo_L3_v1"));  // User needs at least these
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

        // Now delete it
        enrichmentLayoutService.deleteLayoutBySourceId(mainTestTenant.getId(), enrichmentLayout.getSourceId());

        // Check that it's gone
        RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                Collections.singleton(AssertionError.class), null);
        retry.execute(ctx -> {
            EnrichmentLayout removedLayout = enrichmentLayoutService.findBySourceId(mainTestTenant.getId(), enrichmentLayout.getSourceId());
            Assert.assertNull(removedLayout, "EnrichmentLayout should have been removed.");
            return true;
        });
    }

    private EnrichmentLayout makeEnrichmentLayoutObj(List<String> elementList) {

        String layoutId = RandomStringUtils.randomAlphanumeric(8);
        String userEmail = "user@dnb.com";

        EnrichmentLayout layout = new EnrichmentLayout();
        layout.setLayoutId(layoutId);
        layout.setCreatedBy(userEmail);

        layout.setSourceId(getRandomSourceId());
        layout.setDomain(DataDomain.SalesMarketing);
        layout.setRecordType(DataRecordType.Domain);

        layout.setElements(elementList);

        return layout;
    }
}
