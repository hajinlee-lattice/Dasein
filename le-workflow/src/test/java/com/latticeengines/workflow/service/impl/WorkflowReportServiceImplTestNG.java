package com.latticeengines.workflow.service.impl;

import static org.testng.Assert.assertEquals;

import java.util.List;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.entitymgr.ReportEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.workflow.exposed.service.WorkflowReportService;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

public class WorkflowReportServiceImplTestNG extends WorkflowTestNGBase {

    private static final String TENANT1 = "TENANT1";
    private static final String TENANT2 = "TENANT2";
    private static final String REPORT_DATA = "{\"report\": \"abd\" }";

    @Inject
    private ReportEntityMgr reportEntityMgr;

    @Inject
    private WorkflowReportService reportService;

    @Inject
    private TenantService tenantService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        deleteReports();
    }

    @AfterClass(groups = "functional")
    public void tearDown() {
        deleteReports();
    }

    private void deleteReports() {
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

    private Tenant setupTenant(String t) {
        String tenantId = CustomerSpace.parse(t).toString();
        Tenant tenant = tenantService.findByTenantId(tenantId);
        if (tenant != null) {
            setupSecurityContext(tenant);
            tenantService.discardTenant(tenant);
        }
        tenant = new Tenant();
        tenant.setId(tenantId);
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
        assertEquals(reportService.findAll(MultiTenantContext.getTenant().getId()).size(), 0);
        reportService.createOrUpdateReport(MultiTenantContext.getTenant().getId(), report);
        Report retrievedReport = reportService.findReportByName(tenant.getId(), reportName);
        assertEquals(retrievedReport.getName(), reportName);
        String jsonStr = new String(retrievedReport.getJson().getData());
        assertEquals(jsonStr, REPORT_DATA);
    }

}
