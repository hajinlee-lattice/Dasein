package com.latticeengines.matchapi.service.impl;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.customer.CustomerReport;
import com.latticeengines.domain.exposed.datacloud.customer.IncorrectLookupReproduceDetail;
import com.latticeengines.matchapi.service.CustomerReportService;
import com.latticeengines.matchapi.testframework.MatchapiFunctionalTestNGBase;


public class CustomerReportServiceImplTestNG extends MatchapiFunctionalTestNGBase {

    @Autowired
    private CustomerReportService customerReportService;
    private static String tenantName = CustomerReportServiceImplTestNG.class.getSimpleName();
    private static String reportedByUser = "lattice@lattice-engines.com";
    private static String suggestedValue = "test";
    private static String comment = "this is test!";
    private static List<String> matchLog = Collections.singletonList("[00:00:00.000] Started the journey. TravelerId=e19a76d9-c8b5-4751-9de1-bf77cf377017");
    private static Map<String, String> inputKeys = Collections.singletonMap("Domain", "google.com");
    private static Map<String, String> matchedKeys = Collections.singletonMap("LDC_Name", "Alphabet Inc.");

    private CustomerReport customerReport = new CustomerReport();

    @Test(groups = "functional", enabled = true)
    public void testCreateReport() {
        customerReport.setId(UUID.randomUUID().toString());
        customerReport.setComment(comment);
        customerReport.setCreatedTime(new Date());
        customerReport.setMatchLog(matchLog);
        customerReport.setReportedByTenant(tenantName);
        customerReport.setReportedByUser(reportedByUser);
        customerReport.setSuggestedValue(suggestedValue);
        IncorrectLookupReproduceDetail detail = new IncorrectLookupReproduceDetail();
        detail.setInputKeys(inputKeys);
        detail.setMatchedKeys(matchedKeys);
        customerReport.setReproduceDetail(detail);
        customerReportService.saveReport(customerReport);

        CustomerReport report = customerReportService.findById(customerReport.getId());
        Assert.assertNotNull(report);
        Assert.assertEquals(comment, report.getComment());

    }

}
