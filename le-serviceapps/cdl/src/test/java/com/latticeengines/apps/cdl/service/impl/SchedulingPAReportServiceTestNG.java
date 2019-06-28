package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.SchedulingPAReportService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.scheduling.report.CycleReport;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class SchedulingPAReportServiceTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private SchedulingPAReportService service;

    // TODO enable when implementation finishes
    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class, enabled = false)
    private void testSaveCycleReport(CycleReport cycleReport) {
        // only testing cycle report
        int cycleId = service.saveSchedulingResult(cycleReport, null);

        CycleReport savedReport = service.getCycleReport(cycleId);
        Assert.assertNotNull(savedReport, "Saved cycle report should not be null");
        Assert.assertEquals(savedReport.getSummary(), cycleReport.getSummary(),
                "Saved cycle summary should be the same one as input");

        // cleanup
        service.cleanupOldReports(cycleId);
    }

    @DataProvider(name = "saveCycleReport")
    private Object[][] provideSaveCycleReportTestCases() {
        return new Object[][] { //
                { //
                        new CycleReport(-1, System.currentTimeMillis(),
                                new CycleReport.Summary(5, 3, 2, Collections.singletonMap("tenant1", "appId1"), null)) //
                }, //
                // TODO add more test cases
        };
    }

    // TODO add more test suites for range query and other methods
}
