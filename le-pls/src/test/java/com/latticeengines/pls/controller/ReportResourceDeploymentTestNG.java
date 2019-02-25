package com.latticeengines.pls.controller;


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class ReportResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String REPORT_DATA = "{\"report\":\"abd\"}";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupMarketoEloquaTestEnvironment();
    }

    @SuppressWarnings("unchecked")
    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        switchToExternalAdmin();
        List<?> reports = restTemplate.getForObject(getRestAPIHostPort() + "/pls/reports",
                List.class);
        for (Object report : reports) {
            Map<String, String> map = (Map<String, String>) report;
            restTemplate.delete(String.format("%s/pls/reports/%s", getRestAPIHostPort(), map.get("name")));
        }
        reports = restTemplate.getForObject(getRestAPIHostPort() + "/pls/reports",
                List.class);
        assertEquals(reports.size(), 0);
    }

    private SimpleBooleanResponse createOrUpdate() throws Exception {
        KeyValue json = new KeyValue();
        json.setPayload(REPORT_DATA);
        Report report = new Report();
        report.setName("SomeReport");
        report.setPurpose(ReportPurpose.IMPORT_SUMMARY);
        report.setJson(json);
        return restTemplate.postForObject(getRestAPIHostPort() + "/pls/reports",
                report, SimpleBooleanResponse.class);
    }


    @Test(groups = "deployment")
    public void createOrUpdateReportWithAccess() throws Exception {
        switchToExternalAdmin();
        assertTrue(createOrUpdate().isSuccess());
    }

    @Test(groups = "deployment", dependsOnMethods = { "createOrUpdateReportWithAccess" })
    public void findReportByName() throws Exception {
        switchToExternalAdmin();
        Report report = restTemplate.getForObject(getRestAPIHostPort() + "/pls/reports/SomeReport",
                Report.class);
        assertEquals(report.getName(), "SomeReport");
        System.out.println(report);
        String payload = new String(report.getJson().getPayload());
        assertEquals(payload, REPORT_DATA);
    }

    @Test(groups = "deployment")
    public void createOrUpdateReportWithNoAccess() throws Exception {
        switchToExternalUser();
        boolean exception = false;
        try {
            createOrUpdate().isSuccess();
        } catch (Exception e) {
            exception = true;
            assertTrue(e.getMessage().contains("403"));
        }
        assertTrue(exception, "Exception should have been thrown.");
    }
}
