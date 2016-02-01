package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.workflow.exposed.entitymgr.ReportEntityMgr;
import com.latticeengines.workflow.exposed.service.ReportService;

public class ReportServiceImplTestNG extends PlsFunctionalTestNGBase {

    private static final String TENANT1 = "TENANT1";
    private static final String TENANT2 = "TENANT2";
    private static final String REPORT_DATA = "{\"report\": \"abd\" }";

    @Autowired
    private ReportEntityMgr reportEntityMgr;

    @Autowired
    private ReportService reportService;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private TargetMarketEntityMgr targetMarketEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        deleteReports();
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        deleteReports();
    }

    private void deleteReports() {
        setupSecurityContext(tenantService.findByTenantId(TENANT1));
        targetMarketEntityMgr.deleteAll();
        setupSecurityContext(tenantService.findByTenantId(TENANT2));
        targetMarketEntityMgr.deleteAll();

        List<Report> reports = reportEntityMgr.getAll();

        for (Report report : reports) {
            reportEntityMgr.delete(report);
        }
    }

    @DataProvider(name = "tenants")
    public Object[][] getTenants() {
        return new Object[][] { //
        new Object[] { TENANT1, "SomeReport" }, //
                new Object[] { TENANT2, "SomeReport" } //
        };
    }

    private Tenant setupTenant(String t) throws Exception {
        Tenant tenant = tenantService.findByTenantId(t);
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
        tenant = new Tenant();
        tenant.setId(t);
        tenant.setName(t);
        tenantService.registerTenant(tenant);
        return tenant;
    }

    @Test(groups = "functional", dataProvider = "tenants")
    public void createOrUpdateReportWithTenantAndName(String t, String reportName) throws Exception {
        Tenant tenant = setupTenant(t);
        setupSecurityContext(tenant);

        KeyValue json = new KeyValue();
        json.setData(REPORT_DATA.getBytes());
        Report report = new Report();
        report.setName(reportName);
        report.setPurpose(ReportPurpose.IMPORT_SUMMARY);
        report.setJson(json);
        assertEquals(reportService.findAll().size(), 0);
        reportService.createOrUpdateReport(report);
        Report retrievedReport = reportService.getReportByName(reportName);
        assertEquals(retrievedReport.getName(), reportName);
        String jsonStr = new String(retrievedReport.getJson().getData());
        assertEquals(jsonStr, REPORT_DATA);

    }

}
