package com.latticeengines.pls.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReportType;
import com.latticeengines.domain.exposed.datacloud.customer.IncorrectMatchedAttributeReproduceDetail;
import com.latticeengines.domain.exposed.pls.CustomerReportRequest;
import com.latticeengines.domain.exposed.pls.IncorrectLookupReportRequest;
import com.latticeengines.domain.exposed.pls.IncorrectMatchedAttrReportRequest;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.DataCloudService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;


public class DataCloudServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    private Tenant tenant;

    private static String tenantName = DataCloudServiceImplTestNG.class.getSimpleName();
    private static String suggestedValue = "test";
    private static String comment = "this is test!";
    private static List<String> matchLog = Collections.singletonList("[00:00:00.000] Started the journey. TravelerId=e19a76d9-c8b5-4751-9de1-bf77cf377017");
    private static Map<String, String> inputKeys = Collections.singletonMap("Domain", "google.com");
    private static Map<String, String> matchedKeys = Collections.singletonMap("LDC_Name", "Alphabet Inc.");
    private static String attribute = "LDC_PrimaryIndustry";
    private static String matchedValue = "Bad Industry";

    CustomerReportRequest customerReportRequest;
    IncorrectLookupReportRequest lookupRequest = new IncorrectLookupReportRequest();
    IncorrectMatchedAttrReportRequest matchedRequest = new IncorrectMatchedAttrReportRequest();
    private CustomerReport lookupCustomerReport;
    private CustomerReport matchedCustomerReport;
    @Autowired
    private DataCloudService dataCloudService;
    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        tenant = tenantService.findByTenantId(tenantName);

        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
        tenant = new Tenant();
        tenant.setId(tenantName);
        tenant.setName(tenantName);
        tenantEntityMgr.create(tenant);
        MultiTenantContext.setTenant(tenant);
        createCustomerReport(CustomerReportType.LOOkUP);
        createCustomerReport(CustomerReportType.MATCHEDATTRIBUTE);
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        tenant = tenantService.findByTenantId(tenantName);
        tenantService.discardTenant(tenant);
    }

    private void createCustomerReport(CustomerReportType type) {
        lookupRequest.setComment(comment);
        lookupRequest.setCorrectValue(suggestedValue);
        lookupRequest.setInputKeys(inputKeys);
        lookupRequest.setMatchedKeys(matchedKeys);
        lookupRequest.setMatchLog(matchLog);

        matchedRequest.setAttribute(attribute);
        matchedRequest.setComment(comment);
        matchedRequest.setCorrectValue(suggestedValue);
        matchedRequest.setInputKeys(inputKeys);
        matchedRequest.setMatchedKeys(matchedKeys);
        matchedRequest.setMatchedValue(matchedValue);
        matchedRequest.setMatchLog(matchLog);
        if (type == CustomerReportType.LOOkUP) {
            lookupCustomerReport = dataCloudService.reportIncorrectLookup(lookupRequest);
        } else {
            matchedCustomerReport = dataCloudService.reportIncorrectMatchedAttr(matchedRequest);
        }
    }

    @Test(groups = "functional")
    public void testCustomerReport() {
        lookupCustomerReport = dataCloudService.findById(lookupCustomerReport.getId());
        Assert.assertNotNull(lookupCustomerReport);
        Assert.assertEquals(comment, lookupCustomerReport.getComment());
        Assert.assertNotNull(lookupCustomerReport.getReproduceDetail());
        Assert.assertEquals(inputKeys, lookupCustomerReport.getReproduceDetail().getInputKeys());

        matchedCustomerReport = dataCloudService.findById(matchedCustomerReport.getId());
        Assert.assertNotNull(matchedCustomerReport);
        Assert.assertEquals(suggestedValue, matchedCustomerReport.getSuggestedValue());
        Assert.assertNotNull(matchedCustomerReport.getReproduceDetail());
        Assert.assertTrue(matchedCustomerReport.getReproduceDetail() instanceof IncorrectMatchedAttributeReproduceDetail);
        IncorrectMatchedAttributeReproduceDetail detail = (IncorrectMatchedAttributeReproduceDetail) matchedCustomerReport.getReproduceDetail();
        Assert.assertEquals(matchedKeys, detail.getMatchedKeys());
        Assert.assertEquals(attribute,  detail.getAttribute());
    }
}
