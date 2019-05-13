package com.latticeengines.app.exposed.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.app.exposed.service.DataCloudService;
import com.latticeengines.app.testframework.AppFunctionalTestNGBase;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;
import com.latticeengines.domain.exposed.datacloud.customer.CustomerReportType;
import com.latticeengines.domain.exposed.datacloud.customer.IncorrectMatchedAttributeReproduceDetail;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.pls.IncorrectLookupReportRequest;
import com.latticeengines.domain.exposed.pls.IncorrectMatchedAttrReportRequest;


public class DataCloudServiceImplDeploymentTestNG extends AppFunctionalTestNGBase {

    private static String suggestedValue = "test";
    private static String comment = "this is test!";
    private static List<String> matchLog = Collections.singletonList("[00:00:00.000] Started the journey. TravelerId=e19a76d9-c8b5-4751-9de1-bf77cf377017");
    private static Map<String, String> inputKeys = Collections.singletonMap("Domain", "google.com");
    private static Map<String, String> matchedKeys = Collections.singletonMap("LDC_Name", "Alphabet Inc.");
    private static String attribute = "LDC_PrimaryIndustry";
    private static String matchedValue = "Bad Industry";

    private IncorrectLookupReportRequest lookupRequest = new IncorrectLookupReportRequest();
    private IncorrectMatchedAttrReportRequest matchedRequest = new IncorrectMatchedAttrReportRequest();
    private CustomerReport lookupCustomerReport;
    private CustomerReport matchedCustomerReport;

    @Inject
    private DataCloudService dataCloudService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        mainTestTenant = globalAuthFunctionalTestBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        createCustomerReport(CustomerReportType.LOOKUP);
        createCustomerReport(CustomerReportType.MATCHEDATTRIBUTE);
    }

    @Test(groups = "deployment", enabled = true)
    public void testCustomerReport() {
        MultiTenantContext.setTenant(mainTestTenant);
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
        Assert.assertNotEquals(matchedKeys, detail.getMatchedKeys());
        Assert.assertEquals(matchedKeys.get("LDC_Name"), detail.getMatchedKeys().get(MatchKey.Name.toString()));
        Assert.assertEquals(attribute,  detail.getAttribute());
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
        if (type == CustomerReportType.LOOKUP) {
            lookupCustomerReport = dataCloudService.reportIncorrectLookup(lookupRequest);
        } else {
            matchedCustomerReport = dataCloudService.reportIncorrectMatchedAttr(matchedRequest);
        }
    }
}
